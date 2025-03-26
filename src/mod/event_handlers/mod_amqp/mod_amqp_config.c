#include "mod_amqp.h"

static switch_status_t load_command_config(switch_xml_t cfg)
{
	switch_xml_t commands, profile;

	if ((commands = switch_xml_child(cfg, "commands"))) {
		for (profile = switch_xml_child(commands, "profile"); profile; profile = profile->next) {
			const char *name = switch_xml_attr_soft(profile, "name");
			const char *exchange = switch_xml_attr_soft(profile, "exchange");
			const char *queue = switch_xml_attr_soft(profile, "queue");
			const char *routing_key = switch_xml_attr_soft(profile, "routing_key");

			amqp_command_profile_t *command;
			switch_memory_pool_t *pool;

			switch_core_new_memory_pool(&pool);

			if (mod_amqp_command_create(&command, (char *)name, pool) == SWITCH_STATUS_SUCCESS) {
				command->exchange = switch_core_strdup(pool, exchange);
				command->queue = switch_core_strdup(pool, queue);
				command->routing_key = switch_core_strdup(pool, routing_key);

				switch_core_hash_insert(mod_amqp_globals.command_hash, name, command);
			} else {
				switch_core_destroy_memory_pool(&pool);
			}
		}
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t load_logging_config(switch_xml_t cfg)
{
	switch_xml_t logging, profile;

	if ((logging = switch_xml_child(cfg, "logging"))) {
		for (profile = switch_xml_child(logging, "profile"); profile; profile = profile->next) {
			const char *name = switch_xml_attr_soft(profile, "name");
			const char *exchange = switch_xml_attr_soft(profile, "exchange");
			const char *routing_key = switch_xml_attr_soft(profile, "routing_key");
			const char *log_level = switch_xml_attr_soft(profile, "log_level");

			amqp_logging_profile_t *logger;
			switch_memory_pool_t *pool;

			switch_core_new_memory_pool(&pool);

			if (mod_amqp_logging_create(&logger, (char *)name, pool) == SWITCH_STATUS_SUCCESS) {
				logger->exchange = switch_core_strdup(pool, exchange);
				logger->routing_key = switch_core_strdup(pool, routing_key);
				logger->log_level = switch_log_str2level(log_level);

				switch_core_hash_insert(mod_amqp_globals.logging_hash, name, logger);
			} else {
				switch_core_destroy_memory_pool(&pool);
			}
		}
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t load_dialplan_config(switch_xml_t cfg)
{
	switch_xml_t dialplans, profile;
	
	if ((dialplans = switch_xml_child(cfg, "dialplans"))) {
		for (profile = switch_xml_child(dialplans, "profile"); profile; profile = profile->next) {
			const char *name = switch_xml_attr_soft(profile, "name");
			const char *exchange = switch_xml_attr_soft(profile, "exchange");
			const char *routing_key = switch_xml_attr_soft(profile, "routing_key");
			const char *reply_queue = switch_xml_attr_soft(profile, "reply_queue");
			const char *timeout_str = switch_xml_attr_soft(profile, "timeout");
			
			amqp_dialplan_profile_t *dialplan;
			switch_memory_pool_t *pool;
			
			switch_core_new_memory_pool(&pool);
			
			dialplan = switch_core_alloc(pool, sizeof(amqp_dialplan_profile_t));
			dialplan->name = switch_core_strdup(pool, name);
			dialplan->exchange = switch_core_strdup(pool, exchange);
			dialplan->routing_key = switch_core_strdup(pool, routing_key);
			dialplan->reply_queue = switch_core_strdup(pool, reply_queue);
			dialplan->timeout = atoi(timeout_str);
			dialplan->pool = pool;
			
			switch_core_hash_insert(mod_amqp_globals.dialplan_hash, name, dialplan);
		}
	}
	
	return SWITCH_STATUS_SUCCESS;
}