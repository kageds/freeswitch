#ifndef MOD_AMQP_COMMAND_H
#define MOD_AMQP_COMMAND_H

#include <switch.h>
#include "mod_amqp.h"

typedef struct {
	char *name;
	char *exchange;
	char *queue;
	char *routing_key;

	amqp_connection_state_t connection;
	amqp_channel_t channel;

	switch_bool_t running;
	switch_memory_pool_t *pool;
	switch_thread_t *thread;
} amqp_command_profile_t;

// No function declarations here - they're in mod_amqp.h

#endif /* MOD_AMQP_COMMAND_H */