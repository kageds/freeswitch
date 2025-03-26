#ifndef MOD_AMQP_LOGGING_H
#define MOD_AMQP_LOGGING_H

#include "mod_amqp.h"

typedef struct {
	char *name;
	char *exchange;
	char *routing_key;
	switch_log_level_t log_level;

	amqp_connection_state_t connection;
	amqp_channel_t channel;

	switch_bool_t running;
	switch_memory_pool_t *pool;
	switch_queue_t *log_queue;
	switch_thread_t *thread;
} amqp_logging_profile_t;

// switch_status_t mod_amqp_logging_create(char *name, switch_memory_pool_t *pool);
void mod_amqp_logging_destroy(amqp_logging_profile_t **profile);
switch_status_t mod_amqp_logging_load(void);

#endif