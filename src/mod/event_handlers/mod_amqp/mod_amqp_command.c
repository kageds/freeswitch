/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2005-2012, Anthony Minessale II <anthm@freeswitch.org>
 *
 * Version: MPL 1.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 *
 * The Initial Developer of the Original Code is
 * Anthony Minessale II <anthm@freeswitch.org>
 * Portions created by the Initial Developer are Copyright (C)
 * the Initial Developer. All Rights Reserved.
 *
 * Based on mod_skel by
 * Anthony Minessale II <anthm@freeswitch.org>
 *
 * Contributor(s):
 *
 * Daniel Bryars <danb@aeriandi.com>
 * Tim Brown <tim.brown@aeriandi.com>
 * Anthony Minessale II <anthm@freeswitch.org>
 * William King <william.king@quentustech.com>
 * Mike Jerris <mike@jerris.com>
 *
 * mod_amqp.c -- Sends FreeSWITCH events to an AMQP broker
 *
 */

#include "mod_amqp_command.h"
#include "mod_amqp.h"
#include <switch.h>

const char *lookup_value(const char *key);
void process_value(const char *input, char *output, size_t output_size);


static void add_props_headers(cJSON *response, mod_amqp_json_props_t json_props) {
	for (int i = 0; i < json_props.size && json_props.prop[i] != NULL; i++) {
		char key[256], value[256];

		// Extract key and value from "key=value" format
		if (sscanf(json_props.prop[i], "%255[^=]=%255s", key, value) == 2) {
			char final_key[256], final_value[256];

			// If key starts with '#', strip it and use literal, otherwise look it up
			if (key[0] == '#') {
				strcpy(final_key, key + 1);
			} else {
				strcpy(final_key, lookup_value(key));
			}

			// Process value, handling literals, lookup, and concatenation
			process_value(value, final_value, sizeof(final_value));

			cJSON_AddStringToObject(response, final_key, final_value);
		}
	}

}

static switch_status_t execute_command(const char *cmd, const char *args, mod_amqp_json_props_t json_props,
									   switch_core_session_t *session, amqp_connection_state_t connection, int channel,
									   const char *reply_to, char *correlation_id)
{
	switch_stream_handle_t stream = {0};
	amqp_basic_properties_t props = {0};
	char *json_str;
	cJSON *response;

	SWITCH_STANDARD_STREAM(stream);
	switch_api_execute(cmd, args, session, &stream);

	response = cJSON_CreateObject();
	cJSON_AddStringToObject(response, "response", (char *)stream.data);
	add_props_headers(response, json_props);
	cJSON_AddStringToObject(response, "Msg-ID", correlation_id);
	json_str = cJSON_PrintUnformatted(response);

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");
	props.correlation_id = amqp_cstring_bytes(correlation_id);

	amqp_basic_publish(connection, channel, amqp_empty_bytes, amqp_cstring_bytes(reply_to), 0, 0, &props,
					   amqp_cstring_bytes(json_str));

	switch_safe_free(stream.data);
	cJSON_Delete(response);
	free(json_str);
	switch_safe_free(correlation_id);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t build_event(switch_event_t *event, cJSON *headers) {
	cJSON *item = NULL;
	char *key, *value;

	if(!event) {
		return SWITCH_STATUS_FALSE;
	}

    // Iterate over all key-value pairs
    cJSON_ArrayForEach(item, headers) {
		key = item->string;
		value = item->valuestring;
		if (!strcmp(key, "body")) {
			switch_safe_free(event->body);
			event->body = value;
		} else	{
			if(!strcasecmp(key, "Call-ID")) {
				switch_core_session_t *session = NULL;
				if(!zstr(value)) {
					if ((session = switch_core_session_locate(value)) != NULL) {
						switch_channel_t *channel = switch_core_session_get_channel(session);
						switch_channel_event_set_data(channel, event);
						switch_core_session_rwunlock(session);
					}
				}
			}
			switch_event_add_header_string_nodup(event, SWITCH_STACK_BOTTOM, key, value);
		}
	}
	return SWITCH_STATUS_SUCCESS;
}


static switch_status_t response_ok(mod_amqp_json_props_t json_props, switch_core_session_t *session,
							amqp_connection_state_t connection, int channel, const char *reply_to, char *correlation_id)
{
	amqp_basic_properties_t props = {0};
	char *json_str;
	cJSON *response;

	response = cJSON_CreateObject();
	cJSON_AddStringToObject(response, "response", "ok");
	add_props_headers(response, json_props);
	cJSON_AddStringToObject(response, "Msg-ID", correlation_id);
	json_str = cJSON_PrintUnformatted(response);

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");
	props.correlation_id = amqp_cstring_bytes(correlation_id);

	amqp_basic_publish(connection, channel, amqp_empty_bytes, amqp_cstring_bytes(reply_to), 0, 0, &props,
					   amqp_cstring_bytes(json_str));

	cJSON_Delete(response);
	free(json_str);
	switch_safe_free(correlation_id);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t response_baduuid(mod_amqp_json_props_t json_props, switch_core_session_t *session,
							amqp_connection_state_t connection, int channel, const char *reply_to, char *correlation_id)
{
	amqp_basic_properties_t props = {0};
	char *json_str;
	cJSON *response;

	response = cJSON_CreateObject();
	cJSON_AddStringToObject(response, "response", "baduuid");
	add_props_headers(response, json_props);
	cJSON_AddStringToObject(response, "Msg-ID", correlation_id);
	json_str = cJSON_PrintUnformatted(response);

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");
	props.correlation_id = amqp_cstring_bytes(correlation_id);

	amqp_basic_publish(connection, channel, amqp_empty_bytes, amqp_cstring_bytes(reply_to), 0, 0, &props,
					   amqp_cstring_bytes(json_str));

	cJSON_Delete(response);
	free(json_str);
	switch_safe_free(correlation_id);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t response_badarg(mod_amqp_json_props_t json_props, switch_core_session_t *session,
							amqp_connection_state_t connection, int channel, const char *reply_to, char *correlation_id)
{
	amqp_basic_properties_t props = {0};
	char *json_str;
	cJSON *response;

	response = cJSON_CreateObject();
	cJSON_AddStringToObject(response, "response", "badarg");
	add_props_headers(response, json_props);
	cJSON_AddStringToObject(response, "Msg-ID", correlation_id);
	json_str = cJSON_PrintUnformatted(response);

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");
	props.correlation_id = amqp_cstring_bytes(correlation_id);

	amqp_basic_publish(connection, channel, amqp_empty_bytes, amqp_cstring_bytes(reply_to), 0, 0, &props,
					   amqp_cstring_bytes(json_str));

	cJSON_Delete(response);
	free(json_str);
	switch_safe_free(correlation_id);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t pong(mod_amqp_json_props_t json_props, switch_core_session_t *session,
							amqp_connection_state_t connection, int channel, const char *reply_to, char *correlation_id)
{
	amqp_basic_properties_t props = {0};
	char *json_str;
	cJSON *response;

	response = cJSON_CreateObject();
	cJSON_AddStringToObject(response, "pong", "01234567890");
	add_props_headers(response, json_props);
	cJSON_AddStringToObject(response, "Msg-ID", correlation_id);

	json_str = cJSON_PrintUnformatted(response);

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");
	props.correlation_id = amqp_cstring_bytes(correlation_id);

	amqp_basic_publish(connection, channel, amqp_empty_bytes, amqp_cstring_bytes(reply_to), 0, 0, &props,
					   amqp_cstring_bytes(json_str));

	cJSON_Delete(response);
	free(json_str);
	switch_safe_free(correlation_id);
	return SWITCH_STATUS_SUCCESS;
}

static void *SWITCH_THREAD_FUNC command_thread(switch_thread_t *thread, void *data)
{
	mod_amqp_command_profile_t *profile = (mod_amqp_command_profile_t *)data;
	switch_status_t status = SWITCH_STATUS_SUCCESS;
	amqp_boolean_t passive = 0;

	amqp_bytes_t queueName;
	amqp_queue_declare_ok_t *recv_queue;
	struct timeval timeout = {0, 100000}; // 100ms timeout
	amqp_envelope_t envelope;
	amqp_rpc_reply_t result;
	char *routing_key, *message, *correlation_id;
	char *last_field;
	cJSON *command;
	switch_core_session_t *session = NULL;
	switch_event_t *event = NULL;
	

	const char *reply_to;

	//FIXME - needs a more general solution
	char nodename[1024];
    sprintf(nodename, "freeswitch@%s", mod_amqp_globals.hostname);
	while (profile->running) {

		/* Ensure we have an AMQP connection */
		if (!profile->conn_active->state) {
			switch_status_t status;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Amqp no connection- reconnecting...\n");

			status = mod_amqp_connection_open(profile->conn_root, &(profile->conn_active), profile->name,
											  profile->custom_attr);
			if (status != SWITCH_STATUS_SUCCESS) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Profile[%s] failed to connect with code(%d), sleeping for %dms\n", profile->name,
								  status, profile->reconnect_interval_ms);
				switch_sleep(profile->reconnect_interval_ms * 1000);
				continue;
			}

			/* Check if exchange already exists */
#if AMQP_VERSION_MAJOR == 0 && AMQP_VERSION_MINOR >= 6
			amqp_exchange_declare(profile->conn_active->state, 1, amqp_cstring_bytes(profile->exchange),
								  amqp_cstring_bytes("topic"), 0, /* passive */
								  profile->durable,				  /* durable */
								  0,							  /* auto-delete */
								  0, amqp_empty_table);
#else
			amqp_exchange_declare(profile->conn_active->state, 1, amqp_cstring_bytes(profile->exchange),
								  amqp_cstring_bytes("topic"), 0, /* passive */
								  profile->durable,				  /* durable */
								  amqp_empty_table);
#endif

			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state),
										   "Checking for command exchange\n")) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Profile[%s] failed to create missing command exchange\n", profile->name);
				continue;
			}

			/* Ensure we have a queue */
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Creating command queue\n");
			recv_queue =
				amqp_queue_declare(profile->conn_active->state,		  // state
								   1,								  // channel
								   amqp_cstring_bytes(profile->name), // queue name
								   profile->passive, profile->durable, profile->exclusive, profile->auto_delete,
								   amqp_empty_table); // args

			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state), "Declaring queue\n")) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Profile[%s] failed to connect with code(%d), sleeping for %dms\n", profile->name,
								  status, profile->reconnect_interval_ms);
				switch_sleep(profile->reconnect_interval_ms * 1000);
				continue;
			}

			//			if (queueName.bytes) { amqp_bytes_free(queueName); }

			queueName = amqp_bytes_malloc_dup(recv_queue->queue);

			if (!queueName.bytes) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Out of memory while copying queue name");
				break;
			}

			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Created command queue %.*s\n", (int)queueName.len,
							  (char *)queueName.bytes);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Binding command queue to exchange %s\n",
							  profile->exchange);

			/* Bind the queue to the exchange */
			amqp_queue_bind(profile->conn_active->state,			  // state
							1,										  // channel
							queueName,								  // queue
							amqp_cstring_bytes(profile->exchange),	  // exchange
							amqp_cstring_bytes(profile->binding_key), // routing key
							amqp_empty_table);						  // args

			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state), "Binding queue")) {
				mod_amqp_connection_close(profile->conn_active);
				profile->conn_active = NULL;
				switch_sleep(profile->reconnect_interval_ms * 1000);
				continue;
			}

			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Amqp reconnect successful- connected\n");
			continue;
		}

		if (!profile->conn_active || !profile->conn_active->state) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No active connection\n");
			return NULL;
		}

		// Bind queue to exchange if exchange is specified
		if (profile->exchange) {
			amqp_queue_bind(profile->conn_active->state,
							1, // channel
							queueName, amqp_cstring_bytes(profile->exchange),
							amqp_cstring_bytes(profile->name), // use profile name as routing key
							amqp_empty_table);
		}

		// Start consuming
		amqp_basic_consume(profile->conn_active->state,
						   1, // channel
						   queueName, amqp_empty_bytes,
						   0, // no_local
						   1, // no_ack
						   0, // exclusive
						   amqp_empty_table);

		amqp_maybe_release_buffers(profile->conn_active->state);

		result = amqp_consume_message(profile->conn_active->state, &envelope, &timeout, 0);

		if (result.reply_type == AMQP_RESPONSE_NORMAL) {

			switch_malloc(routing_key, sizeof(char) * envelope.routing_key.len + 1);
			memcpy(routing_key, envelope.routing_key.bytes, envelope.routing_key.len);
			routing_key[envelope.routing_key.len] = '\0';

			switch_malloc(message, sizeof(char) * envelope.message.body.len + 1);
			memcpy(message, envelope.message.body.bytes, envelope.message.body.len);
			message[envelope.message.body.len] = '\0';

			command = cJSON_Parse(message);

			last_field = strrchr(routing_key, '.');

			reply_to = (char *)envelope.message.properties.reply_to.bytes;
			switch_malloc(correlation_id, sizeof(char) * envelope.message.properties.correlation_id.len + 1);
			memcpy(correlation_id, envelope.message.properties.correlation_id.bytes,
						   envelope.message.properties.correlation_id.len);
			correlation_id[envelope.message.properties.correlation_id.len] = '\0';

			if (command && last_field) {
				last_field++; // Move past the dot
				// Check if the last field is either "api" or "ping"
				if (!strcmp(last_field, "api") || !strcmp(last_field, "bgapi")) {
					cJSON *cmd_obj = cJSON_GetObjectItem(command, "command");
					cJSON *args_obj = cJSON_GetObjectItem(command, "args");
					cJSON *nodename_obj = cJSON_GetObjectItem(command, "Switch-Nodename");
					const char *nodename_str;

					if (nodename_obj && nodename_obj->valuestring) {
						nodename_str = nodename_obj->valuestring;
						if (zstr_buf(nodename_str) || strcmp(nodename_str, nodename)) {
							switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Received %s for %s ignore\n",
									  last_field, nodename_str);
							continue;
						}
					}
					if (cmd_obj && cmd_obj->valuestring) {
						const char *cmd = cmd_obj->valuestring;
						const char *args = (args_obj && args_obj->valuestring) ? args_obj->valuestring : "";
						execute_command(cmd, args, profile->props, session, profile->conn_active->state,
										1, // channel
										reply_to, correlation_id);
					} else {
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
										  "Received AMQP command with missing 'command' field\n");
					}
					cJSON_Delete(command);
				} else if (!strcmp(last_field, "ping")) {
					cJSON *nodename_obj = cJSON_GetObjectItem(command, "Switch-Nodename");
					const char *nodename_str;
					if (nodename_obj && nodename_obj->valuestring) {
						nodename_str = nodename_obj->valuestring;
						if (zstr_buf(nodename_str) || strcmp(nodename_str, nodename)) {
							switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Received ping for %s ignore\n",
									  nodename_str);
							continue;
						}
					}
					pong(profile->props, session, profile->conn_active->state,
						 1, // channel
						 reply_to, correlation_id);
				} else if (!strcmp(last_field, "sendmsg")) {
					cJSON *uuid_obj = cJSON_GetObjectItem(command, "UUID");
					cJSON *headers_obj = cJSON_GetObjectItem(command, "FSHeaders");
					cJSON *nodename_obj = cJSON_GetObjectItem(command, "Switch-Nodename");
					const char *uuid_str;
					const char *nodename_str;

					if (uuid_obj && uuid_obj->valuestring) {
						uuid_str = uuid_obj->valuestring;
						if (zstr_buf(uuid_str) || !(session = switch_core_session_locate(uuid_str))) {
							response_baduuid(profile->props, session, profile->conn_active->state,
						 							1, // channel
						 							reply_to, correlation_id);
							goto err;
						}
					}

					if (nodename_obj && nodename_obj->valuestring) {
						nodename_str = nodename_obj->valuestring;
						if (zstr_buf(nodename_str) || strcmp(nodename_str, nodename)) {
							switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Received sendcmd for %s ignore\n",
									  nodename_str);
							continue;
						}
					}

					switch_event_create(&event, SWITCH_EVENT_SEND_MESSAGE);
					if (build_event(event, headers_obj) == SWITCH_STATUS_SUCCESS) {
						switch_core_session_queue_private_event(session, &event, SWITCH_FALSE);
						switch_core_session_rwunlock(session);
						response_ok(profile->props, session, profile->conn_active->state,
									1, // channel
									reply_to, correlation_id);
					} else {
						response_badarg(profile->props, session, profile->conn_active->state,
									1, // channel
									reply_to, correlation_id);
						switch_core_session_rwunlock(session);
						goto err;

					}
				} else {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Received Unknown AMQP command %s\n",
									  last_field);
				}
			}
err:
			switch_safe_free(routing_key);
			switch_safe_free(message);
			amqp_destroy_envelope(&envelope);
		}
	}
	amqp_bytes_free(queueName);
	return NULL;
}

switch_status_t mod_amqp_command_create(char *name, switch_xml_t cfg)
{
	mod_amqp_command_profile_t *profile;
	int arg = 0, i = 0;

	switch_memory_pool_t *pool;
	char *exchange = NULL, *exchange_type = NULL, *content_type = NULL, *binding_key = NULL;
	int exchange_durable = 1; /* durable */
	int delivery_mode = -1;
	int delivery_timestamp = 1;

	switch_xml_t params, param, connections, connection, props, prop;

	if (switch_core_new_memory_pool(&pool) != SWITCH_STATUS_SUCCESS) { goto err; }
	profile = switch_core_alloc(pool, sizeof(mod_amqp_command_profile_t));

	profile = switch_core_alloc(pool, sizeof(mod_amqp_xml_fetch_profile_t));
	profile->pool = pool;
	profile->name = switch_core_strdup(profile->pool, name);
	profile->running = 1;

	profile->conn_root = NULL;
	profile->conn_active = NULL;

	if ((params = switch_xml_child(cfg, "params")) != NULL) {
		for (param = switch_xml_child(params, "param"); param; param = param->next) {
			char *var = (char *)switch_xml_attr_soft(param, "name");
			char *val = (char *)switch_xml_attr_soft(param, "value");

			if (!var) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Profile[%s] param missing 'name' attribute\n",
								  profile->name);
				continue;
			}

			if (!val) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT,
								  "Profile[%s] param[%s] missing 'value' attribute\n", profile->name, var);
				continue;
			}

			if (!strncmp(var, "reconnect_interval_ms", 21)) {
				int interval = atoi(val);
				if (interval && interval > 0) { profile->reconnect_interval_ms = interval; }
			} else if (!strncmp(var, "exchange-type", 13)) {
				exchange_type = switch_core_strdup(profile->pool, val);
			} else if (!strncmp(var, "exchange-name", 13)) {
				exchange = switch_core_strdup(profile->pool, val);
			} else if (!strncmp(var, "binding-key", 11)) {
				binding_key = switch_core_strdup(profile->pool, val);
			} else if (!strncmp(var, "exchange-durable", 16)) {
				exchange_durable = switch_true(val);
			} else if (!strncmp(var, "delivery-mode", 13)) {
				delivery_mode = atoi(val);
			} else if (!strncmp(var, "delivery-timestamp", 18)) {
				delivery_timestamp = switch_true(val);
			} else if (!strncmp(var, "exchange_type", 13)) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
								  "Found exchange_type parameter. please change to exchange-type\n");
			} else if (!strncmp(var, "exchange", 8)) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
								  "Found exchange parameter. please change to exchange-name\n");
			} else if (!strncmp(var, "content-type", 12)) {
				content_type = switch_core_strdup(profile->pool, val);
			}
		}
	}

	/* Handle defaults of string types */
	profile->exchange = exchange ? exchange : switch_core_strdup(profile->pool, "TAP.Commands");
	profile->exchange_type = exchange_type ? exchange_type : switch_core_strdup(profile->pool, "topic");
	profile->exchange_durable = exchange_durable;
	profile->binding_key = binding_key ? binding_key : switch_core_strdup(profile->pool, "commandBindingKey");
	profile->delivery_mode = delivery_mode;
	profile->delivery_timestamp = delivery_timestamp;
	profile->content_type =
		content_type ? content_type : switch_core_strdup(profile->pool, MOD_AMQP_DEFAULT_CONTENT_TYPE);

	if ((connections = switch_xml_child(cfg, "connections")) != NULL) {
		for (connection = switch_xml_child(connections, "connection"); connection; connection = connection->next) {
			if (!profile->conn_root) {
				if (mod_amqp_connection_create(&(profile->conn_root), connection, profile->pool) !=
					SWITCH_STATUS_SUCCESS) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
									  "Profile[%s] failed to create connection\n", profile->name);
					continue;
				}
				profile->conn_active = profile->conn_root;
			} else {
				if (mod_amqp_connection_create(&(profile->conn_active->next), connection, profile->pool) !=
					SWITCH_STATUS_SUCCESS) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
									  "Profile[%s] failed to create connection\n", profile->name);
					continue;
				}
				profile->conn_active = profile->conn_active->next;
			}
		}
	}

	if ((props = switch_xml_child(cfg, "props")) != NULL) {
		for (prop = switch_xml_child(props, "prop"); prop; prop = prop->next) {
			char *var = (char *)switch_xml_attr_soft(prop, "name");
			char *val = (char *)switch_xml_attr_soft(prop, "value");
			int len = strlen(var) + strlen(val) + 2;
			profile->props.prop[profile->props.size] = switch_core_alloc(pool, len);
			switch_snprintf(profile->props.prop[profile->props.size], len, "%s=%s", var, val);
			/* increment size because the count returned the number of separators, not number of fields */
			profile->props.size++;
			profile->props.prop[profile->props.size] = NULL;
		}
	}

	// Start command processing thread
	profile->command_thread = switch_core_alloc(pool, sizeof(switch_thread_t *));
	{
		switch_threadattr_t *thd_attr;
		switch_threadattr_create(&thd_attr, pool);
		switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
		switch_thread_create(&profile->command_thread, thd_attr, command_thread, profile, pool);
	}

	switch_core_hash_insert(mod_amqp_globals.command_hash, name, profile);

	return SWITCH_STATUS_SUCCESS;
err:
	mod_amqp_command_destroy(&profile);
	return SWITCH_STATUS_GENERR;
}

switch_status_t mod_amqp_command_destroy(mod_amqp_command_profile_t **profile)
{
	mod_amqp_command_profile_t *p = *profile;
	switch_status_t status;

	if (p) {
		p->running = SWITCH_FALSE;
		if (p->command_thread) { switch_thread_join(&status, p->command_thread); }

		if (p->pool) { switch_core_destroy_memory_pool(&p->pool); }

		*profile = NULL;
	}

	return SWITCH_STATUS_SUCCESS;
}

// Function to process values, handling literals and lookup, and concatenating on comma
void process_value(const char *input, char *output, size_t output_size)
{
	char temp[256];
	char *token;

	// Properly initialize 'temp' with 'input' before using strtok
	strncpy(temp, input, sizeof(temp) - 1);
	temp[sizeof(temp) - 1] = '\0'; // Ensure null termination

	token = strtok(temp, ","); // Now safe to tokenize
	output[0] = '\0';		   // Initialize output as empty

	while (token) {
		char processed_part[128];

		if (token[0] == '#') {
			strcpy(processed_part, token + 1); // Strip '#' and use as is
		} else {
			strncpy(processed_part, lookup_value(token), sizeof(processed_part) - 1);
			processed_part[sizeof(processed_part) - 1] = '\0';
		}

		// Append processed part to output safely
		if (output[0] != '\0') {
			strncat(output, processed_part, output_size - strlen(output) - 1);
		} else {
			strncpy(output, processed_part, output_size - 1);
			output[output_size - 1] = '\0';
		}

		token = strtok(NULL, ",");
	}
}

// Mock lookup function (Replace this with actual lookup logic)
const char *lookup_value(const char *key)
{
	if (strcmp(key, "AMQP-HOSTNAME") == 0) return mod_amqp_globals.hostname;
	return "unknown"; // Default if not found
}

SWITCH_MOD_DECLARE(switch_status_t) mod_amqp_command_load(void) { return SWITCH_STATUS_SUCCESS; }

/* For Emacs:
 * Local Variables:
 * mode:c
 * indent-tabs-mode:t
 * tab-width:4
 * c-basic-offset:4
 * End:
 * For VIM:
 * vim:set softtabstop=4 shiftwidth=4 tabstop=4
 */
