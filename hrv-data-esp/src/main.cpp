/**
 * MAX30102 PPG Processor — On-Device Edition
 * ===========================================
 * Performs full PPG signal processing on the ESP32. HR, HRV (RMSSD), and SpO2 are computed
 * on-device and published as computed metrics rather than raw samples.
 *
 * ARCHITECTURE (three FreeRTOS tasks, two cores)
 * -----------------------------------------------
 *
 *   Core 1 — taskSensor  (priority 3, highest)
 *   +--------------------------------------------------+
 *   |  Polls MAX30102 FIFO at 100Hz                    |
 *   |  Detects finger presence / absence               |
 *   |  Pushes individual Sample structs -> sampleQueue |
 *   +---------------------+----------------------------+
 *                         | sampleQueue
 *   Core 1 — taskProcessor  (priority 2)
 *   +---------------------v----------------------------+
 *   |  DC removal (exponential moving average)         |
 *   |  Biquad bandpass filter (0.5 - 4.0 Hz)          |
 *   |  Peak detection (threshold + refractory period)  |
 *   |  IBI / HR / RMSSD computation                    |
 *   |  SpO2 from Red/IR AC+DC ratio                    |
 *   |  Pushes Metrics structs -> metricsQueue          |
 *   +---------------------+----------------------------+
 *                         | metricsQueue
 *   Core 0 — taskPublisher  (priority 1, lowest)
 *   +---------------------v----------------------------+
 *   |  Serialises Metrics to JSON                      |
 *   |  Publishes to MQTT broker                        |
 *   |  Handles WiFi / MQTT reconnects                  |
 *   +--------------------------------------------------+
 *
 * SIGNAL PROCESSING NOTES
 * -----------------------
 * DC removal:  y[n] = x[n] - alpha * x[n-1] + alpha * y[n-1]
 *              alpha = 0.95 at 100 Hz gives ~3 Hz high-pass corner
 *
 * Bandpass:    Cascaded biquad IIR, 2nd order Butterworth
 *              Low cut:  0.5 Hz (removes respiration drift)
 *              High cut: 4.0 Hz (removes motion above 240 BPM)
 *              Coefficients pre-computed for 100 Hz sample rate
 *
 * Peak detect: Adaptive threshold = 60% of recent signal range
 *              Refractory period = 400 ms (max 150 BPM)
 *              Confirmed only when signal falls below threshold again
 *
 * SpO2:        R = (AC_red / DC_red) / (AC_ir / DC_ir)
 *              SpO2 = 110.0 - 25.0 * R  (empirical linear calibration)
 *
 * MQTT Payload (published once per UPDATE_INTERVAL_MS):
 *   {
 *     "time":     1740400800,  // Unix UTC seconds
 *     "hr":       61.2,        // Heart rate (BPM)
 *     "rmssd":    42.5,        // HRV RMSSD (ms), -1 if insufficient beats
 *     "spo2":     97.1,        // SpO2 (%), -1 if insufficient data
 *     "ibi":      [980, 1020], // Last IBI_PUBLISH_COUNT inter-beat intervals (ms)
 *     "beats":    24           // Total beats detected this session
 *   }
 *
 * Wiring (I2C):
 *   MAX30102 SDA -> ESP32 GPIO 21
 *   MAX30102 SCL -> ESP32 GPIO 22
 *   MAX30102 VIN -> 3.3V
 *   MAX30102 GND -> GND
 */

#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <time.h>
#include <math.h>
#include "MAX30105.h"
#include "config.h"

// ---------------------------------------------------------------------------
// Sensor configuration
// ---------------------------------------------------------------------------
static const byte SAMPLE_RATE = 100;     // Hz — must match filter coefficients
static const byte LED_BRIGHTNESS = 0xFF; // 50mA
static const int PULSE_WIDTH = 215;      // us — 16-bit ADC
static const int ADC_RANGE = 16384;
static const long IR_FINGER_THRESHOLD = 50000;

// ---------------------------------------------------------------------------
// Signal processing parameters
// ---------------------------------------------------------------------------

// DC removal: higher alpha = more low-frequency content retained
// 0.95 at 100Hz gives ~3Hz high-pass corner, removes DC baseline
static const float DC_ALPHA = 0.95f;

// Bandpass filter: 0.5–4.0 Hz (Butterworth 2nd order, Fs=100Hz)
// Coefficients computed with: scipy.signal.butter(2, [0.5, 4.0], fs=100, btype='band')
// b = [0.0663, 0.0, -0.0663]   (numerator — note b[1]=0 exactly)
// a = [1.0, -1.7820, 0.8674]   (denominator)
static const float BP_B0 = 0.06632f;
static const float BP_B1 = 0.00000f;
static const float BP_B2 = -0.06632f;
static const float BP_A1 = -1.78203f;
static const float BP_A2 = 0.86736f;

// Peak detection
static const uint32_t REFRACTORY_MS = 400; // min ms between beats (150 BPM max)
static const float PEAK_THRESHOLD = 0.60f; // fraction of recent signal range
static const int ADAPT_WINDOW = 200;       // samples for adaptive threshold (2s)

// IBI validity limits
static const uint32_t MIN_IBI_MS = 300;  // 200 BPM max
static const uint32_t MAX_IBI_MS = 2000; // 30 BPM min

// HRV: minimum beats before RMSSD is reported
static const int MIN_BEATS_FOR_HRV = 6;

// SpO2: number of samples to average AC/DC components over
static const int SPO2_WINDOW = 100; // 1 second at 100Hz

// How many IBIs to include in each MQTT publish
static const int IBI_PUBLISH_COUNT = 8;

// How often to publish metrics (ms). 5000ms = every 5 seconds.
static const uint32_t UPDATE_INTERVAL_MS = 5000;

// ---------------------------------------------------------------------------
// FreeRTOS sizing
// ---------------------------------------------------------------------------
static const int SAMPLE_QUEUE_DEPTH = 50; // ~0.5s buffer of individual samples
static const int METRICS_QUEUE_DEPTH = 4; // small — publisher is fast

static const int SENSOR_TASK_STACK = 3072;
static const int PROCESSOR_TASK_STACK = 6144; // larger: filter state + IBI buffer
static const int PUBLISHER_TASK_STACK = 6144;

static const int SENSOR_TASK_PRIORITY = 3;
static const int PROCESSOR_TASK_PRIORITY = 2;
static const int PUBLISHER_TASK_PRIORITY = 1;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

// One raw sensor sample — pushed from taskSensor to taskProcessor
struct Sample
{
    uint32_t ts_ms; // millis() when read from FIFO
    long ir;        // raw IR ADC count
    long red;       // raw Red ADC count
};

// Computed metrics — pushed from taskProcessor to taskPublisher
struct Metrics
{
    uint32_t unix_time;              // UTC seconds
    float hr_bpm;                    // heart rate
    float rmssd_ms;                  // HRV RMSSD (-1 = not enough beats)
    float spo2_pct;                  // SpO2 (-1 = not enough data)
    uint32_t ibi[IBI_PUBLISH_COUNT]; // recent IBIs in ms
    int ibi_count;                   // valid entries in ibi[]
    uint32_t total_beats;            // session beat count
};

// ---------------------------------------------------------------------------
// Shared FreeRTOS queues (only shared objects between tasks)
// ---------------------------------------------------------------------------
static QueueHandle_t sampleQueue;
static QueueHandle_t metricsQueue;

// Sensor accessed from taskSensor only
static MAX30105 sensor;

// HiveMQ client — TLS. setInsecure() skips certificate verification,
// which is acceptable for research/prototyping but should be replaced
// with a proper CA cert bundle for production use.
static WiFiClientSecure hiveEspClient;
static PubSubClient mqttClient(hiveEspClient);

// ---------------------------------------------------------------------------
// NTP sync
// ---------------------------------------------------------------------------
void sync_ntp()
{
    Serial.print("Syncing NTP...");
    configTime(0, 0, "pool.ntp.org", "time.nist.gov");
    struct tm timeinfo;
    unsigned long start = millis();
    while (!getLocalTime(&timeinfo))
    {
        if (millis() - start > 10000)
        {
            Serial.println(" FAILED -- timestamps will be 0.");
            return;
        }
        delay(200);
        Serial.print(".");
    }
    Serial.printf(" OK -- %04d-%02d-%02d %02d:%02d:%02d UTC\n",
                  timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
                  timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec);
}

// ---------------------------------------------------------------------------
// WiFi setup
// ---------------------------------------------------------------------------
void setup_wifi()
{
    WiFi.mode(WIFI_STA);
    Serial.printf("Connecting to %s", WIFI_SSID);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    while (WiFi.status() != WL_CONNECTED)
    {
        delay(500);
        Serial.print(".");
    }
    WiFi.setSleep(false); // disable modem sleep to prevent beacon-miss gaps
    Serial.printf("\nWiFi connected -- IP: %s\n",
                  WiFi.localIP().toString().c_str());
}

// ---------------------------------------------------------------------------
// MQTT reconnect — HiveMQ Cloud (taskPublisher only)
// ---------------------------------------------------------------------------
void mqtt_reconnect()
{
    if (mqttClient.connected())
        return;
    Serial.print("HiveMQ connecting...");
    if (mqttClient.connect(MQTT_CLIENT,
                           MQTT_USER,
                           MQTT_PASS))
    {
        Serial.println(" connected!");
    }
    else
    {
        Serial.printf(" failed (rc=%d), retrying in 3s\n", mqttClient.state());
        vTaskDelay(pdMS_TO_TICKS(3000));
    }
}

// ---------------------------------------------------------------------------
// Sensor init (taskSensor only)
// ---------------------------------------------------------------------------
void init_sensor()
{
    if (!sensor.begin(Wire, I2C_SPEED_FAST))
    {
        Serial.println("ERROR: MAX30102 not found. Check wiring.");
        while (true)
        {
            vTaskDelay(pdMS_TO_TICKS(1000));
        }
    }
    sensor.setup(LED_BRIGHTNESS, 1, 2, SAMPLE_RATE, PULSE_WIDTH, ADC_RANGE);
    Serial.printf("MAX30102 ready -- %dHz\n", SAMPLE_RATE);
    Serial.println("Waiting for finger...");
}

// ---------------------------------------------------------------------------
// TASK: Sensor sampling (Core 1, priority 3)
//
// Reads the MAX30102 FIFO and pushes individual Sample structs onto
// sampleQueue. Has zero signal processing — its only job is to get
// samples off the sensor and into the queue as fast as possible.
// ---------------------------------------------------------------------------
void taskSensor(void *pvParameters)
{
    init_sensor();

    bool fingerPresent = false;

    while (true)
    {
        sensor.check(); // burst-read FIFO into library ring buffer

        while (sensor.available())
        {
            long irValue = sensor.getFIFOIR();
            long redValue = sensor.getFIFORed();
            sensor.nextSample();

            bool fingerNow = (irValue >= IR_FINGER_THRESHOLD);
            if (fingerNow != fingerPresent)
            {
                fingerPresent = fingerNow;
                Serial.println(fingerPresent
                                   ? "Finger detected -- processing started."
                                   : "Finger removed  -- processing paused.");

                // Notify processor of state change via a sentinel sample
                // with ts_ms=0, which the processor treats as a reset signal.
                if (!fingerPresent)
                {
                    Sample sentinel = {0, 0, 0};
                    xQueueSend(sampleQueue, &sentinel, 0);
                }
            }

            if (!fingerPresent)
                continue;

            Sample s;
            s.ts_ms = millis();
            s.ir = irValue;
            s.red = redValue;

            // Drop sample rather than block if processor is falling behind
            if (xQueueSend(sampleQueue, &s, 0) != pdTRUE)
            {
                // Queue full — processor overloaded or stalled
            }
        }

        taskYIELD();
    }
}

// ---------------------------------------------------------------------------
// TASK: Signal processor (Core 1, priority 2)
//
// Consumes Sample structs from sampleQueue, runs the full PPG processing
// pipeline, and emits Metrics structs to metricsQueue at UPDATE_INTERVAL_MS.
//
// All state is local to this task — no shared memory, no mutexes needed.
// ---------------------------------------------------------------------------
void taskProcessor(void *pvParameters)
{

    // -- Biquad filter state (Direct Form II) --------------------------------
    // w[0] and w[1] are the delay elements of the filter.
    // Separate state for IR and Red channels.
    float ir_w1 = 0.0f, ir_w2 = 0.0f;
    float red_w1 = 0.0f, red_w2 = 0.0f;

    // -- DC removal state ----------------------------------------------------
    float ir_dc = 0.0f;
    float red_dc = 0.0f;

    // -- SpO2 accumulators ---------------------------------------------------
    // We accumulate AC^2 (RMS approximation) and DC over SPO2_WINDOW samples
    float ir_ac_sq_sum = 0.0f;
    float red_ac_sq_sum = 0.0f;
    float ir_dc_sum = 0.0f;
    float red_dc_sum = 0.0f;
    int spo2_count = 0;
    float last_spo2 = -1.0f;

    // -- Peak detection state ------------------------------------------------
    float peak_threshold = 0.0f;  // adaptive, updated each sample
    float signal_max = 0.0f;      // rolling max over ADAPT_WINDOW
    float signal_min = 0.0f;      // rolling min over ADAPT_WINDOW
    bool above_threshold = false; // true when signal is above threshold
    float peak_value = 0.0f;      // highest point seen above threshold
    uint32_t last_beat_ms = 0;    // millis() of last confirmed beat
    uint32_t last_beat_ts = 0;    // ts_ms of last confirmed beat (for IBI)

    // -- IBI / HRV state -----------------------------------------------------
    static const int IBI_BUF_SIZE = 32;
    uint32_t ibi_buf[IBI_BUF_SIZE]; // circular buffer of recent IBIs
    int ibi_head = 0;
    int ibi_count = 0; // valid entries (up to IBI_BUF_SIZE)
    uint32_t total_beats = 0;

    // -- Publish timing ------------------------------------------------------
    uint32_t last_publish_ms = 0;

    // -- Adaptive signal range buffer ----------------------------------------
    // Ring buffer of recent filtered values for adaptive threshold
    static const int RANGE_BUF = ADAPT_WINDOW;
    float range_buf[RANGE_BUF];
    int range_idx = 0;
    memset(range_buf, 0, sizeof(range_buf));

    Sample s;

    while (true)
    {
        // Block until a sample arrives (up to 500ms timeout)
        if (xQueueReceive(sampleQueue, &s, pdMS_TO_TICKS(500)) != pdTRUE)
        {
            continue;
        }

        // Sentinel: ts_ms=0 means finger removed — reset all state
        if (s.ts_ms == 0)
        {
            ir_w1 = ir_w2 = red_w1 = red_w2 = 0.0f;
            ir_dc = red_dc = 0.0f;
            ir_ac_sq_sum = red_ac_sq_sum = 0.0f;
            ir_dc_sum = red_dc_sum = 0.0f;
            spo2_count = 0;
            last_spo2 = -1.0f;
            peak_threshold = signal_max = signal_min = 0.0f;
            above_threshold = false;
            peak_value = 0.0f;
            last_beat_ms = last_beat_ts = 0;
            ibi_head = ibi_count = 0;
            total_beats = 0;
            last_publish_ms = 0;
            memset(range_buf, 0, sizeof(range_buf));
            range_idx = 0;
            continue;
        }

        float ir_raw = (float)s.ir;
        float red_raw = (float)s.red;

        // -- Step 1: DC removal ----------------------------------------------
        // High-pass filter: removes slowly varying DC baseline.
        // y[n] = x[n] - x[n-1] + alpha * y[n-1]
        float ir_ac = ir_raw - ir_dc;
        float red_ac = red_raw - red_dc;
        ir_dc = ir_raw - ir_ac * (1.0f - DC_ALPHA);
        red_dc = red_raw - red_ac * (1.0f - DC_ALPHA);

        // -- Step 2: Bandpass filter (IR channel for beat detection) ---------
        // Direct Form II biquad:
        //   w    = x - a1*w1 - a2*w2
        //   y    = b0*w + b1*w1 + b2*w2
        //   w2   = w1,  w1 = w
        float w = ir_ac - BP_A1 * ir_w1 - BP_A2 * ir_w2;
        float filt = BP_B0 * w + BP_B1 * ir_w1 + BP_B2 * ir_w2;
        ir_w2 = ir_w1;
        ir_w1 = w;

        // Same filter on Red channel (for SpO2 AC component)
        float rw = red_ac - BP_A1 * red_w1 - BP_A2 * red_w2;
        float rfilt = BP_B0 * rw + BP_B1 * red_w1 + BP_B2 * red_w2;
        red_w2 = red_w1;
        red_w1 = rw;

        // -- Step 3: Adaptive threshold update --------------------------------
        // Keep a rolling window of recent filtered values to track signal
        // amplitude, then set threshold at PEAK_THRESHOLD fraction of range.
        range_buf[range_idx] = filt;
        range_idx = (range_idx + 1) % RANGE_BUF;

        signal_max = range_buf[0];
        signal_min = range_buf[0];
        for (int i = 1; i < RANGE_BUF; i++)
        {
            if (range_buf[i] > signal_max)
                signal_max = range_buf[i];
            if (range_buf[i] < signal_min)
                signal_min = range_buf[i];
        }
        peak_threshold = signal_min + PEAK_THRESHOLD * (signal_max - signal_min);

        // -- Step 4: Peak detection ------------------------------------------
        // State machine: wait for signal to rise above threshold (above_threshold=true),
        // track the highest point, then confirm the peak when signal falls back below.
        // Enforce refractory period to suppress dicrotic notch.
        uint32_t now_ms = s.ts_ms;

        if (!above_threshold)
        {
            if (filt > peak_threshold &&
                (now_ms - last_beat_ms) > REFRACTORY_MS)
            {
                above_threshold = true;
                peak_value = filt;
            }
        }
        else
        {
            // Track peak while signal stays above threshold
            if (filt > peak_value)
            {
                peak_value = filt;
            }
            // Confirm beat when signal falls back below threshold
            if (filt < peak_threshold)
            {
                above_threshold = false;

                // Compute IBI and validate
                if (last_beat_ts > 0)
                {
                    uint32_t ibi = now_ms - last_beat_ts;

                    // Step 1: hard physiological limits
                    bool valid = (ibi >= MIN_IBI_MS && ibi <= MAX_IBI_MS);

                    // Step 2: median-based outlier rejection.
                    // If we have at least 4 previous beats, compute their
                    // median and reject if this IBI deviates more than 30%.
                    // This catches false peaks from motion or signal noise
                    // that slip through the refractory period.
                    if (valid && ibi_count >= 4)
                    {
                        // Copy last min(ibi_count, 8) IBIs for median calc
                        const int MED_N = 8;
                        uint32_t tmp[MED_N];
                        int n = (ibi_count < MED_N) ? ibi_count : MED_N;
                        for (int i = 0; i < n; i++)
                        {
                            int idx = ((ibi_head - 1 - i) + IBI_BUF_SIZE) % IBI_BUF_SIZE;
                            tmp[i] = ibi_buf[idx];
                        }
                        // Simple insertion sort for small n
                        for (int i = 1; i < n; i++)
                        {
                            uint32_t key = tmp[i];
                            int j = i - 1;
                            while (j >= 0 && tmp[j] > key)
                            {
                                tmp[j + 1] = tmp[j];
                                j--;
                            }
                            tmp[j + 1] = key;
                        }
                        uint32_t median = (n % 2 == 0)
                                              ? (tmp[n / 2 - 1] + tmp[n / 2]) / 2
                                              : tmp[n / 2];

                        // Reject if deviation > 30% of median
                        float dev = fabsf((float)ibi - (float)median) / (float)median;
                        if (dev > 0.30f)
                        {
                            valid = false;
                            Serial.printf("[PROC] IBI rejected: %ums (median=%u dev=%.0f%%)\n",
                                          ibi, median, dev * 100.0f);
                        }
                    }

                    if (valid)
                    {
                        ibi_buf[ibi_head] = ibi;
                        ibi_head = (ibi_head + 1) % IBI_BUF_SIZE;
                        if (ibi_count < IBI_BUF_SIZE)
                            ibi_count++;
                        total_beats++;
                    }
                }

                last_beat_ms = now_ms;
                last_beat_ts = now_ms;
            }
        }

        // -- Step 5: SpO2 accumulation ---------------------------------------
        ir_ac_sq_sum += filt * filt;
        red_ac_sq_sum += rfilt * rfilt;
        ir_dc_sum += ir_dc;
        red_dc_sum += red_dc;
        spo2_count++;

        if (spo2_count >= SPO2_WINDOW)
        {
            // R = (AC_rms_red / DC_red) / (AC_rms_ir / DC_ir)
            float ir_ac_rms = sqrtf(ir_ac_sq_sum / SPO2_WINDOW);
            float red_ac_rms = sqrtf(red_ac_sq_sum / SPO2_WINDOW);
            float ir_dc_avg = ir_dc_sum / SPO2_WINDOW;
            float red_dc_avg = red_dc_sum / SPO2_WINDOW;

            if (ir_dc_avg > 1.0f && red_dc_avg > 1.0f)
            {
                float R = (red_ac_rms / red_dc_avg) / (ir_ac_rms / ir_dc_avg);
                // Empirical calibration: SpO2 = 110 - 25*R (standard linear approx)
                last_spo2 = 110.0f - 25.0f * R;
                last_spo2 = fminf(100.0f, fmaxf(70.0f, last_spo2));
            }

            // Reset accumulators for next window (sliding, non-overlapping)
            ir_ac_sq_sum = red_ac_sq_sum = 0.0f;
            ir_dc_sum = red_dc_sum = 0.0f;
            spo2_count = 0;
        }

        // -- Step 6: Publish metrics at regular interval ---------------------
        if ((now_ms - last_publish_ms) >= UPDATE_INTERVAL_MS && ibi_count >= 1)
        {
            Metrics m;
            m.unix_time = (uint32_t)time(nullptr);
            m.total_beats = total_beats;
            m.spo2_pct = last_spo2;

            // HR from mean of recent IBIs
            int n = min(ibi_count, IBI_BUF_SIZE);
            float ibi_sum = 0.0f;
            for (int i = 0; i < n; i++)
            {
                int idx = ((ibi_head - 1 - i) + IBI_BUF_SIZE) % IBI_BUF_SIZE;
                ibi_sum += ibi_buf[idx];
            }
            float mean_ibi = ibi_sum / n;
            m.hr_bpm = 60000.0f / mean_ibi;

            // RMSSD from successive IBI differences
            if (ibi_count >= MIN_BEATS_FOR_HRV)
            {
                float sq_sum = 0.0f;
                int pairs = min(ibi_count - 1, IBI_BUF_SIZE - 1);
                for (int i = 0; i < pairs; i++)
                {
                    int idx1 = ((ibi_head - 1 - i) + IBI_BUF_SIZE) % IBI_BUF_SIZE;
                    int idx2 = ((ibi_head - 2 - i) + IBI_BUF_SIZE) % IBI_BUF_SIZE;
                    float diff = (float)ibi_buf[idx1] - (float)ibi_buf[idx2];
                    sq_sum += diff * diff;
                }
                m.rmssd_ms = sqrtf(sq_sum / pairs);
            }
            else
            {
                m.rmssd_ms = -1.0f;
            }

            // Copy most recent IBIs for publish
            m.ibi_count = min(ibi_count, IBI_PUBLISH_COUNT);
            for (int i = 0; i < m.ibi_count; i++)
            {
                int idx = ((ibi_head - 1 - i) + IBI_BUF_SIZE) % IBI_BUF_SIZE;
                m.ibi[i] = ibi_buf[idx];
            }

            // Push to publisher (non-blocking — drop if publisher is stalled)
            if (xQueueSend(metricsQueue, &m, 0) != pdTRUE)
            {
                Serial.println("WARN: metricsQueue full -- metrics dropped");
            }

            Serial.printf("[PROC] HR=%.1f RMSSD=%.1f SpO2=%.1f beats=%lu\n",
                          m.hr_bpm, m.rmssd_ms, m.spo2_pct, m.total_beats);

            last_publish_ms = now_ms;
        }
    }
}

// ---------------------------------------------------------------------------
// TASK: MQTT publisher (Core 0, priority 1)
//
// Waits for Metrics structs and publishes them as JSON. Handles
// WiFi/MQTT reconnects without affecting the processing pipeline.
// ---------------------------------------------------------------------------
void taskPublisher(void *pvParameters)
{
    // TLS without certificate verification — acceptable for research.
    // For production, replace with hiveEspClient.setCACert(root_ca).
    hiveEspClient.setInsecure();
    mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
    mqttClient.setBufferSize(512);
    mqtt_reconnect();

    Metrics m;

    while (true)
    {
        if (xQueueReceive(metricsQueue, &m, pdMS_TO_TICKS(200)) == pdTRUE)
        {

            if (!mqttClient.connected())
                mqtt_reconnect();

            StaticJsonDocument<512> doc;
            doc["time"] = m.unix_time;
            doc["hr"] = serialized(String(m.hr_bpm, 1));
            doc["rmssd"] = serialized(String(m.rmssd_ms, 1));
            doc["spo2"] = serialized(String(m.spo2_pct, 1));
            doc["beats"] = m.total_beats;
            JsonArray ibiArr = doc.createNestedArray("ibi");
            for (int i = 0; i < m.ibi_count; i++)
                ibiArr.add(m.ibi[i]);

            char jsonBuffer[512];
            serializeJson(doc, jsonBuffer);

            bool ok = mqttClient.publish(MQTT_TOPIC, jsonBuffer);
            Serial.printf("[MQTT %s] %s\n", ok ? "OK  " : "FAIL", jsonBuffer);
        }

        mqttClient.loop();
    }
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------
void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("\n--- MAX30102 On-Device Processor ---");

    setup_wifi();
    sync_ntp();

    // Create queues
    sampleQueue = xQueueCreate(SAMPLE_QUEUE_DEPTH, sizeof(Sample));
    metricsQueue = xQueueCreate(METRICS_QUEUE_DEPTH, sizeof(Metrics));
    if (!sampleQueue || !metricsQueue)
    {
        Serial.println("FATAL: queue creation failed");
        while (true)
        {
            delay(1000);
        }
    }

    // Sensor task — Core 1, highest priority
    xTaskCreatePinnedToCore(taskSensor, "Sensor", SENSOR_TASK_STACK,
                            NULL, SENSOR_TASK_PRIORITY, NULL, 1);

    // Processor task — Core 1, medium priority
    xTaskCreatePinnedToCore(taskProcessor, "Processor", PROCESSOR_TASK_STACK,
                            NULL, PROCESSOR_TASK_PRIORITY, NULL, 1);

    // Publisher task — Core 0, lowest priority
    xTaskCreatePinnedToCore(taskPublisher, "Publisher", PUBLISHER_TASK_STACK,
                            NULL, PUBLISHER_TASK_PRIORITY, NULL, 0);

    Serial.println("Tasks created.");
}

void loop()
{
    vTaskSuspend(NULL);
}