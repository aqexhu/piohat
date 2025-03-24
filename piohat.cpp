/*
 * piohat - AQEX Raspberry Pi IO HAT module MQTT connector service
 *
 * Copyright (C) 2025 Balazs Kiss / AQEX Kft.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


#include <gpiod.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <time.h>
#include <mosquitto.h>
#include <queue>
#include <mutex>
#include <condition_variable>

// MQTT settings
#define MQTT_HOST "192.168.199.192" // Change to your MQTT broker address
#define MQTT_PORT 1883
#define MQTT_TOPIC_PUB "piohat-module/in"
#define MQTT_TOPIC_SUB "piohat-module/out"
#define MQTT_CLIENT_ID "piohat"

// Queue for command communication
std::queue<std::string> commandQueue;
std::mutex queueMutex;
std::condition_variable queueCondition;

// Mosquitto client
struct mosquitto *mosq = NULL;

pthread_t g_thread, g_shdthread, mqttThreadId;

#define IN1_PIN 17
#define IN2_PIN 27
#define OUT1_PIN 14
#define OUT2_PIN 15

struct gpiod_chip *chip;
struct gpiod_line *lineIN1;
struct gpiod_line *lineIN2;
u_int8_t lastval_IN1 = 255, lastval_IN2 = 255;
bool event_IN1, event_IN2;
struct gpiod_line *lineOUT1;
struct gpiod_line *lineOUT2;
struct timespec ts;
struct timespec start_time, test_time;

#define CONSUMER "piohat-module"
#define POLLINTERVAL 1000

void qseSwitchRelay(uint8_t relayNo, uint8_t state)
{
    syslog(LOG_INFO, "Switching relay %d - %d.", relayNo, state);

    switch (relayNo)
    {
    case 1:
        gpiod_line_set_value(lineOUT1, state);
        syslog(LOG_INFO, "Line OUT1 %d.", state);
        break;
    case 2:
        gpiod_line_set_value(lineOUT2, state);
        syslog(LOG_INFO, "Line OUT2 %d.", state);
        break;

    default:
        syslog(LOG_ERR, "Unknown output: %d - state %d.", relayNo, state);
        break;
    }
}

// MQTT publish/subscribe thread
void *mqttThread(void *arg)
{
    (void)arg; // Suppress unused parameter warning

    mosquitto_lib_init();
    mosq = mosquitto_new(MQTT_CLIENT_ID, true, NULL);

    if (!mosq)
    {
        syslog(LOG_ERR, "Error: Out of memory.");
        fprintf(stderr, "Error: Out of memory.\n");
        return NULL;
    }

    if (mosquitto_connect(mosq, MQTT_HOST, MQTT_PORT, 60) != MOSQ_ERR_SUCCESS)
    {
        syslog(LOG_ERR, "Error: Could not connect to MQTT broker.");
        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();
        return NULL;
    }

    mosquitto_subscribe(mosq, NULL, MQTT_TOPIC_SUB, 0);

    mosquitto_loop_start(mosq);

    // Callback for received messages
    mosquitto_message_callback_set(mosq, [](struct mosquitto *mosq, void *userdata, const struct mosquitto_message *message)
                                   {
        (void)userdata;
        if (message->payloadlen) {
            printf("MQTT Received: %s\n", (char *)message->payload);
            if (strcmp((const char *)message->payload, "INTERRUPT_COMMAND_OUT1:1") == 0) {
                qseSwitchRelay(1, 1);
            } else if (strcmp((const char *)message->payload, "INTERRUPT_COMMAND_OUT1:0") == 0) {
                qseSwitchRelay(1, 0);
            } else if (strcmp((const char *)message->payload, "INTERRUPT_COMMAND_OUT2:1") == 0) {
                qseSwitchRelay(2, 1);
            } else if (strcmp((const char *)message->payload, "INTERRUPT_COMMAND_OUT2:0") == 0) {
                qseSwitchRelay(2, 0);
            } else {
	        syslog(LOG_ERR, "Unknown command: %s", (const char *)message->payload);
            }
        } else {
	    syslog(LOG_INFO, "MQTT Message received: %s.", (const char *)message->payload);
        } });

    while (1)
    {
        std::string command;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCondition.wait(lock, []
                                { return !commandQueue.empty(); });
            command = commandQueue.front();
            commandQueue.pop();
        }

        syslog(LOG_INFO, "MQTT Thread sending command: %s", command.c_str());
        mosquitto_publish(mosq, NULL, MQTT_TOPIC_PUB, command.length(), command.c_str(), 0, false);
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return NULL;
}

double diffcltime(timespec a, timespec b)
{
    // Calculate the elapsed time in nanoseconds
    long long elapsed_nanoseconds = (b.tv_sec - a.tv_sec) * 1000000000LL +
                                    (b.tv_nsec - a.tv_nsec);

    // Convert nanoseconds to microseconds
    return (double)(elapsed_nanoseconds / 1000.0);
}

void pushCommandToQ(std::string cmd)
{
    std::string command = "INTERRUPT_COMMAND_" + cmd;

    {
        std::unique_lock<std::mutex> lock(queueMutex);
        commandQueue.push(command);
    }
    queueCondition.notify_one();
    printf("Interrupt Handler: Command enqueued: %s\n", command.c_str());
}

void *g_callback(void *args)
{
    struct gpiod_line_bulk bulk[2], events[2];
    struct gpiod_line_event ev_g;

    gpiod_line_bulk_add(bulk, lineIN1);
    gpiod_line_bulk_add(bulk, lineIN2);

    while (true)
    {
        if (gpiod_line_event_wait_bulk(bulk, &ts, events))
        {
            syslog(LOG_INFO, "Event.");

            for (u_int8_t i = 0; i < gpiod_line_bulk_num_lines(events); i++)
            {
                struct gpiod_line *line;
                line = gpiod_line_bulk_get_line(events, i);
                if (!line)
                {
                    syslog(LOG_ERR, "Unable to get line %d\n", i);
                    continue;
                }
                gpiod_line_event_read(line, &ev_g);
                if (line == lineIN1)
                {
                    syslog(LOG_INFO, "Line IN1 event.");

                    if (ev_g.event_type == GPIOD_LINE_EVENT_FALLING_EDGE)
                    {
                        if (lastval_IN1 != gpiod_line_get_value(lineIN1))
                        {
                            syslog(LOG_INFO, "Line IN1/Turnstile1 0.");
                            pushCommandToQ(std::string("IN1:0"));
                        }
                    }
                    else if (ev_g.event_type == GPIOD_LINE_EVENT_RISING_EDGE)
                    {
                        if (lastval_IN1 != gpiod_line_get_value(lineIN1))
                        {
                            syslog(LOG_INFO, "Line IN1/Turnstile1 1.");
                            pushCommandToQ(std::string("IN1:1"));
                        }
                    }
                    lastval_IN1 = gpiod_line_get_value(lineIN1);
                }
                else if (line == lineIN2)
                {
                    syslog(LOG_INFO, "Line IN2 event.");

                    if (ev_g.event_type == GPIOD_LINE_EVENT_FALLING_EDGE)
                    {
                        clock_gettime(CLOCK_MONOTONIC, &start_time);
                        syslog(LOG_INFO, "Line IN2/Turnstile2 0.");
                        pushCommandToQ(std::string("IN2:0"));
                        continue;
                    }
                    else if (ev_g.event_type == GPIOD_LINE_EVENT_RISING_EDGE)
                    {
                        syslog(LOG_INFO, "Line IN2/Turnstile2 1.");
                        pushCommandToQ(std::string("IN2:1"));
                    }
                    lastval_IN2 = gpiod_line_get_value(lineIN2);
                }
                fflush(stdout);
                usleep(POLLINTERVAL);
            }
        }
    }
}

int g_gpiorelease()
{
    gpiod_chip_close(chip);
    return 0;
}

int g_gpioinit()
{
    const char *chipname = "gpiochip0";
    chip = gpiod_chip_open_by_name(chipname);
    if (!chip)
    {
        chipname = "gpiochip4";
        chip = gpiod_chip_open_by_name(chipname);
        if (!chip)
        {
            syslog(LOG_ERR, "Open chip failed\n");
            exit(0);
        }
    }
    syslog(LOG_INFO, "Chip name: %s - label: %s - %d lines\n", gpiod_chip_name(chip), gpiod_chip_label(chip), gpiod_chip_num_lines(chip));
    lineIN1 = gpiod_chip_get_line(chip, IN1_PIN);
    lineIN2 = gpiod_chip_get_line(chip, IN2_PIN);
    lineOUT1 = gpiod_chip_get_line(chip, OUT1_PIN);
    lineOUT2 = gpiod_chip_get_line(chip, OUT2_PIN);
    gpiod_line_request_output(lineOUT1, CONSUMER, 0);
    gpiod_line_request_output(lineOUT2, CONSUMER, 0);

    ts.tv_sec = 10;
    ts.tv_nsec = 0;

    lastval_IN1 = gpiod_line_get_value(lineIN1);
    int retv = gpiod_line_request_both_edges_events_flags(lineIN1, CONSUMER, GPIOD_LINE_REQUEST_FLAG_BIAS_DISABLE);
    if (retv == -1)
    {
        syslog(LOG_ERR, "IN1 line event request failed: %s", strerror(errno));
    }
    else
    {
        if (gpiod_line_get_value(lineIN1) == 0)
        {
            syslog(LOG_INFO, "Line IN1/Turnstile1 0.");
        }
        else
        {
            syslog(LOG_INFO, "Line IN1/Turnstile1 1.");
        }
    }

    retv = gpiod_line_request_both_edges_events_flags(lineIN2, CONSUMER, GPIOD_LINE_REQUEST_FLAG_BIAS_DISABLE);
    if (retv == -1)
    {
        syslog(LOG_ERR, "IN2 line event request failed: %s", strerror(errno));
    }
    else
    {
        if (gpiod_line_get_value(lineIN2) == 0)
        {
            syslog(LOG_INFO, "Line IN2/Turnstile2 0.");
        }
        else
        {
            syslog(LOG_INFO, "Line IN2/Turnstile2 1.");
        }
    }

    return 0;
}

int g_gpio_events()
{
    if (pthread_create(&g_thread, NULL, &g_callback, NULL) != 0)
    {
        syslog(LOG_ERR, "Thread GPIO init failed.");
        return -1;
    }
    return 0;
}

int g_mqtt_thread()
{
    if (pthread_create(&mqttThreadId, NULL, mqttThread, NULL) != 0)
    {
        syslog(LOG_ERR, "Thread MQTT init failed.");
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    openlog(CONSUMER, LOG_PID | LOG_NDELAY, LOG_USER);

    syslog(LOG_INFO, "Used pins (BCM) - IN1: %d, IN2: %d, OUT1: %d, OUT2: %d", IN1_PIN, IN2_PIN, OUT1_PIN, OUT2_PIN);
    printf("Used pins (BCM) - IN1: %d, IN2: %d, OUT1: %d, OUT2: %d", IN1_PIN, IN2_PIN, OUT1_PIN, OUT2_PIN);

    if (!g_gpioinit())
    {
        if (!g_mqtt_thread())
        {
            if (!g_gpio_events())
            {
                pthread_join(g_thread, NULL);
                g_gpiorelease();
            }
            pthread_join(mqttThreadId, NULL);
        }
    }

    closelog();
    return 0;
}
