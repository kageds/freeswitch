#include "mod_amqp.h"
#include <switch.h>
#include <switch_json.h>
#include <sys/time.h>

static const char *fetch_uuid_sources[] = {"Fetch-Call-UUID", "refer-from-channel-id", "sip_call_id", NULL};

static mod_amqp_message_t *create_fetch_xml_request(mod_amqp_xml_fetch_profile_t *profile, const char *section,
													const char *tag_name, const char *key_name, const char *key_value,
													switch_event_t *params)
{
	mod_amqp_message_t *msg;
	cJSON *json;
	cJSON *vars;
	switch_event_header_t *hp;
	const char *uuid = switch_event_get_header(params, "Fetch-UUID");

	switch_malloc(msg, sizeof(mod_amqp_message_t));

	json = cJSON_CreateObject();
	cJSON_AddStringToObject(json, "call_uuid", uuid);
	cJSON_AddStringToObject(json, "action", "request");
	cJSON_AddStringToObject(json, "section", section);
	cJSON_AddStringToObject(json, "tag_name", tag_name);
	cJSON_AddStringToObject(json, "key_name", key_name);
	cJSON_AddStringToObject(json, "key_value", key_value);

	vars = cJSON_CreateObject();
	for (hp = params->headers; hp; hp = hp->next) { cJSON_AddStringToObject(vars, hp->name, hp->value); }
	cJSON_AddItemToObject(json, "variables", vars);

	msg->pjson = cJSON_PrintUnformatted(json);
	snprintf(msg->routing_key, sizeof(msg->routing_key), "FreeSWITCH.%s.request.%s", section, uuid);

	cJSON_Delete(json);
	return msg;
}

switch_xml_t mod_amqp_fetch_xml_section(const char *section, const char *tag_name, const char *key_name,
										const char *key_value, switch_event_t *params, void *user_data)
{
	switch_xml_t xml = NULL;
	switch_uuid_t uuid;
	switch_time_t now = 0;
	switch_event_t *event = params;
	xml_fetch_reply_t reply, *pending, *prev = NULL;
	//	const char *uuid_str = switch_event_get_header(event, "Unique-ID");
	mod_amqp_xml_fetch_profile_t *profile = (mod_amqp_xml_fetch_profile_t *)user_data;
	const char *fetch_call_id;

	// FIXME - needs a more general solution
	char nodename[1024];

	mod_amqp_message_t *msg;
	now = switch_micro_time_now();

	if (!profile) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No profile specified\n");
		return NULL;
	}

	// Ensure we have an active connection
	if (!profile->conn_active) {
		if (mod_amqp_fetch_xml_connect(profile) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to connect to AMQP\n");
			return NULL;
		}
	}

	if (event == NULL) {
		if (switch_event_create(&event, SWITCH_EVENT_GENERAL) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "error creating event for fetch handler\n");
			return xml;
		}
	}

	/* prepare the reply collector */
	switch_uuid_get(&uuid);
	switch_uuid_format(reply.uuid_str, &uuid);
	reply.next = NULL;
	reply.xml_str = NULL;

	if (switch_event_get_header(event, "Unique-ID") == NULL) {
		int i;
		for (i = 0; fetch_uuid_sources[i] != NULL; i++) {
			if ((fetch_call_id = switch_event_get_header(event, fetch_uuid_sources[i])) != NULL) {
				switch_core_session_t *session = NULL;
				if ((session = switch_core_session_locate(fetch_call_id)) != NULL) {
					switch_channel_t *channel = switch_core_session_get_channel(session);
					uint32_t verbose = switch_channel_test_flag(channel, CF_VERBOSE_EVENTS);
					switch_channel_set_flag(channel, CF_VERBOSE_EVENTS);
					switch_channel_event_set_data(channel, event);
					switch_channel_set_flag_value(channel, CF_VERBOSE_EVENTS, verbose);
					switch_core_session_rwunlock(session);
					break;
				}
			}
		}
	}

	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Fetch-UUID", reply.uuid_str);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Fetch-Section", section);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Fetch-Tag", tag_name);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Fetch-Key-Name", key_name);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Fetch-Key-Value", key_value);
	sprintf(nodename, "freeswitch@%s", mod_amqp_globals.hostname);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Switch-Nodename", nodename);
	switch_event_add_header(event, SWITCH_STACK_BOTTOM, "Fetch-Timeout", "%u", profile->fetch_timeout);
	switch_event_add_header(event, SWITCH_STACK_BOTTOM, "Fetch-Timestamp-Micro", "%" SWITCH_UINT64_T_FMT,
							(uint64_t)now);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Kazoo-Version", VERSION);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Kazoo-Bundle", BUNDLE);
	switch_event_add_header_string(event, SWITCH_STACK_BOTTOM, "Kazoo-Release", RELEASE);

	// Create message and add to queue
	msg = create_fetch_xml_request(profile, section, tag_name, key_name, key_value, event);

	/* add our reply placeholder to the replies list */
	switch_mutex_lock(profile->replies_mutex);
	if (!profile->replies) {
		profile->replies = &reply;
	} else {
		reply.next = profile->replies;
		profile->replies = &reply;
	}
	switch_mutex_unlock(profile->replies_mutex);

	if (msg) {
		switch (mod_amqp_fetch_xml_send(profile, msg)) {
		case SWITCH_STATUS_SUCCESS:
			mod_amqp_util_msg_destroy(&msg);
			break;

		case SWITCH_STATUS_NOT_INITALIZED:
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "XML fetch send failed with 'not initialized'\n");
			break;

		case SWITCH_STATUS_SOCKERR:
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "XML fetch send failed with 'socket error'\n");
			break;

		default:
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "XML fetch send failed with a generic error\n");
			break;
		}
	}

	/* wait for a reply (if there isnt already one...amazingly improbable but lets not take shortcuts */
	switch_mutex_lock(profile->replies_mutex);

	if (!reply.xml_str) {
		switch_time_t timeout;

		timeout = switch_micro_time_now() + 3000000;
		while (switch_micro_time_now() < timeout) {
			/* unlock the replies list and go to sleep, calculate a three second timeout before we started the loop
			 * plus 100ms to add a little hysteresis between the timeout and the while loop */
			switch_thread_cond_timedwait(profile->new_reply, profile->replies_mutex,
										 (timeout - switch_micro_time_now() + 100000));

			/* if we woke up (and therefore have locked replies again) check if we got our reply
			 * otherwise we either timed-out (the while condition will fail) or one of
			 * our sibling processes got a reply and we should go back to sleep */
			if (reply.xml_str) { break; }
		}
	}
	/* find our reply placeholder in the linked list and remove it */
	pending = profile->replies;
	while (pending != NULL) {
		if (pending->uuid_str == reply.uuid_str) { break; }

		prev = pending;
		pending = pending->next;
	}

	if (pending) {
		if (!prev) {
			profile->replies = reply.next;
		} else {
			prev->next = reply.next;
		}
	}

	/* we are done with the replies link-list */
	switch_mutex_unlock(profile->replies_mutex);
	/* after all that did we get what we were after?! */
	if (reply.xml_str) {
		/* HELL YA WE DID */
		// if (kazoo_globals.expand_headers_on_fetch) {
		//	reply.xml_str = expand_vars(reply.xml_str);
		// }
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Received %s XML (%s) after %dms: %s\n", section,
						  reply.uuid_str, (unsigned int)(switch_micro_time_now() - now) / 1000, reply.xml_str);
		if ((xml = switch_xml_parse_str_dynamic(reply.xml_str, SWITCH_FALSE)) == NULL) {
			switch_safe_free(reply.xml_str);
		}
	} else {
		/* facepalm */
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "Request for %s XML (%s) timed-out after %dms\n",
						  section, reply.uuid_str, (unsigned int)(switch_micro_time_now() - now) / 1000);
	}

	return xml;
}

switch_status_t mod_amqp_fetch_xml_connect(mod_amqp_xml_fetch_profile_t *profile)
{
	switch_status_t status;
	amqp_boolean_t passive = 0;
	amqp_boolean_t durable = 1;

	if (!profile->conn_active) {
		status = mod_amqp_connection_open(profile->conn_root, &(profile->conn_active), profile->name, NULL);
		if (status == SWITCH_STATUS_SUCCESS) {

#if AMQP_VERSION_MAJOR == 0 && AMQP_VERSION_MINOR >= 6
			amqp_exchange_declare(profile->conn_active->state, 1, amqp_cstring_bytes(profile->exchange),
								  amqp_cstring_bytes(profile->exchange_type), passive, profile->exchange_durable,
								  profile->exchange_auto_delete, 0, amqp_empty_table);
#else
			amqp_exchange_declare(profile->conn_active->state, 1, amqp_cstring_bytes(profile->exchange),
								  amqp_cstring_bytes(profile->exchange_type), passive, profile->exchange_durable,
								  amqp_empty_table);
#endif
			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state), "Declaring exchange")) {
				mod_amqp_connection_close(profile->conn_active);
				profile->conn_active = NULL;
				return SWITCH_STATUS_FALSE;
			}
			return SWITCH_STATUS_SUCCESS;
		}
	}
	return SWITCH_STATUS_FALSE;
}

switch_status_t mod_amqp_fetch_xml_create(char *name, switch_xml_t cfg)
{
	mod_amqp_xml_fetch_profile_t *profile = NULL;
	int arg = 0, i = 0;
	char *argv[SWITCH_EVENT_ALL];
	switch_xml_t params, param, connections, connection;
	switch_threadattr_t *thd_attr = NULL;
	char *exchange = NULL, *exchange_type = NULL, *content_type = NULL;
	switch_bool_t exchange_durable = FALSE, exchange_auto_delete = TRUE;
	switch_bool_t queue_durable = FALSE, queue_auto_delete = TRUE;
	int delivery_mode = -1;
	int delivery_timestamp = 1;
	switch_memory_pool_t *pool;
	char *format_fields[MAX_ROUTING_KEY_FORMAT_FIELDS + 1];
	int format_fields_size = 0;

	memset(format_fields, 0, (MAX_ROUTING_KEY_FORMAT_FIELDS + 1) * sizeof(char *));

	if (switch_core_new_memory_pool(&pool) != SWITCH_STATUS_SUCCESS) { goto err; }

	profile = switch_core_alloc(pool, sizeof(mod_amqp_xml_fetch_profile_t));
	profile->pool = pool;
	profile->name = switch_core_strdup(profile->pool, name);
	profile->running = 1;
	memset(profile->format_fields, 0, (MAX_ROUTING_KEY_FORMAT_FIELDS + 1) * sizeof(mod_amqp_keypart_t));
	profile->conn_root = NULL;
	profile->conn_active = NULL;
	profile->replies = NULL;
	profile->replies_mutex = NULL;
	profile->requests_mutex = NULL;
	switch_mutex_init(&profile->replies_mutex, SWITCH_MUTEX_DEFAULT, pool);
	switch_thread_cond_create(&profile->new_reply, pool);

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
			} else if (!strncmp(var, "exchange-durable", 16)) {
				exchange_durable = switch_true(val);
			} else if (!strncmp(var, "exchange-auto-delete", 20)) {
				exchange_auto_delete = switch_true(val);
			} else if (!strncmp(var, "queue-durable", 16)) {
				queue_durable = switch_true(val);
			} else if (!strncmp(var, "queue-auto-delete", 17)) {
				queue_auto_delete = switch_true(val);
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
			} else if (!strncmp(var, "format_fields", 13)) {
				char *tmp = switch_core_strdup(profile->pool, val);
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "amqp format fields : %s\n", tmp);
				if ((format_fields_size = mod_amqp_count_chars(tmp, ',')) >= MAX_ROUTING_KEY_FORMAT_FIELDS) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT,
									  "You can have only %d routing fields in the routing key.\n",
									  MAX_ROUTING_KEY_FORMAT_FIELDS);
					goto err;
				}

				format_fields_size++;
				switch_separate_string(tmp, ',', format_fields, MAX_ROUTING_KEY_FORMAT_FIELDS);
				format_fields[format_fields_size] = NULL;
			}
		}
	}

	/* Handle defaults of string types */
	profile->exchange = exchange ? exchange : switch_core_strdup(profile->pool, "TAP.Directory");
	profile->exchange_type = exchange_type ? exchange_type : switch_core_strdup(profile->pool, "topic");
	profile->exchange_durable = exchange_durable;
	profile->exchange_auto_delete = exchange_auto_delete;
	profile->queue_durable = queue_durable;
	profile->queue_auto_delete = queue_auto_delete;
	profile->delivery_mode = delivery_mode;
	profile->delivery_timestamp = delivery_timestamp;
	profile->reply_queue = switch_core_strdup(profile->pool, "directory.reply");
	profile->send_queue_size = 5000;
	profile->content_type =
		content_type ? content_type : switch_core_strdup(profile->pool, MOD_AMQP_DEFAULT_CONTENT_TYPE);
	for (i = 0; i < format_fields_size; i++) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "amqp routing key %d : %s\n", i, format_fields[i]);
	}

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
	profile->conn_active = NULL;

	if (switch_queue_create(&(profile->send_queue), profile->send_queue_size, profile->pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot create send queue of size %d!\n",
						  profile->send_queue_size);
		goto err;
	}

	if (switch_core_hash_insert(mod_amqp_globals.directory_hash, name, (void *)profile) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
						  "Failed to insert new profile [%s] into mod_amqp profile hash\n", name);
		goto err;
	}

	switch_threadattr_create(&thd_attr, profile->pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	if (switch_thread_create(&profile->fetch_xml_thread, thd_attr, mod_amqp_fetch_xml_thread, profile, profile->pool)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot create 'amqp fetch_xml sender' thread!\n");
		goto err;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Profile[%s] Successfully started\n", profile->name);
	return SWITCH_STATUS_SUCCESS;

err:
	mod_amqp_fetch_xml_destroy(&profile);
	return SWITCH_STATUS_GENERR;
}

switch_status_t mod_amqp_fetch_xml_destroy(mod_amqp_xml_fetch_profile_t **profile)
{
	mod_amqp_xml_fetch_profile_t *dp = *profile;
	if (dp) {
		dp->running = 0;
		if (dp->conn_active) {
			mod_amqp_connection_close(dp->conn_active);
			dp->conn_active = NULL;
		}

		if (dp->pool) { switch_core_destroy_memory_pool(&dp->pool); }

		*profile = NULL;
	}
	return SWITCH_STATUS_SUCCESS;
}

void *SWITCH_THREAD_FUNC mod_amqp_fetch_xml_thread(switch_thread_t *thread, void *data)
{
	mod_amqp_message_t *msg = NULL;
	switch_status_t status = SWITCH_STATUS_SUCCESS;
	mod_amqp_xml_fetch_profile_t *profile = (mod_amqp_xml_fetch_profile_t *)data;
	amqp_bytes_t queuename;
	amqp_queue_declare_ok_t *recv_queue;
	struct timeval timeout = {0, 100000}; // 100ms timeout
	amqp_envelope_t envelope;
	amqp_rpc_reply_t result;
	xml_fetch_reply_t *reply;
	char *message;
	cJSON *json;

	while (profile->running) {
		/* Ensure we have an AMQP connection */ 
		if (!profile->conn_active || !profile->conn_active->state) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "AMQP no connection - reconnecting...\n");

			status = mod_amqp_connection_open(profile->conn_root, &(profile->conn_active), profile->name,
											  profile->custom_attr);
			if (status != SWITCH_STATUS_SUCCESS) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Profile[%s] failed to connect with code(%d), sleeping for %dms\n", profile->name,
								  status, profile->reconnect_interval_ms);
				switch_sleep(profile->reconnect_interval_ms * 1000);
				continue;
			}
			// Ensure that the exchange exists, and is of the correct type
#if AMQP_VERSION_MAJOR == 0 && AMQP_VERSION_MINOR >= 6
			amqp_exchange_declare(profile->conn_active->state, 1,
								  amqp_cstring_bytes(profile->exchange), amqp_cstring_bytes(profile->exchange_type),
								  0,  /* passive */
								  profile->exchange_durable,
								  profile->exchange_auto_delete,
								  0,
								  amqp_empty_table);
#else
			amqp_exchange_declare(profile->conn_active->state, 1, amqp_cstring_bytes(profile->exchange),
								  amqp_cstring_bytes(profile->exchange_type), passive, profile->exchange_durable,
								  amqp_empty_table);
#endif
			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state),
											"Declaring exchange")) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Profile[%s] failed to create missing command exchange\n", profile->name);
				continue;
			}
			/* Ensure we have a queue */
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Creating command queue\n");
			recv_queue = amqp_queue_declare(profile->conn_active->state,
										   1,								  // channel
										   amqp_cstring_bytes(profile->name), // use profile name as queue name
										   0,								  // passive
										   profile->queue_durable,			  // durable
										   0,								  // exclusive
										   profile->queue_auto_delete,		  // auto-delete
										   amqp_empty_table);
			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state), "Declaring queue\n")) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  "Profile[%s] failed to connect with code(%d), sleeping for %dms\n", profile->name,
								  status, profile->reconnect_interval_ms);
				switch_sleep(profile->reconnect_interval_ms * 1000);
				continue;
			}

			queuename = amqp_bytes_malloc_dup(recv_queue->queue);
			
			if (!queuename.bytes) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Out of memory while copying queue name");
				break;
			}
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Created fetch queue %.*s\n", (int)queuename.len,
							  (char *)queuename.bytes);
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Binding fetch queue to exchange %s\n",
							  profile->exchange);

			amqp_queue_bind(profile->conn_active->state,
							1, // channel
							queuename,
							amqp_cstring_bytes(profile->exchange),
							amqp_cstring_bytes("KAZOO.*.*.*"), // use profile name as routing key
							amqp_empty_table);

			if (mod_amqp_log_if_amqp_error(amqp_get_rpc_reply(profile->conn_active->state), "Binding queue")) {
				mod_amqp_connection_close(profile->conn_active);
				profile->conn_active = NULL;
				switch_sleep(profile->reconnect_interval_ms * 1000);
				continue;
			}

			// Start consuming
			amqp_basic_consume(profile->conn_active->state,
							   1, // channel
							   queuename, amqp_empty_bytes,
							   0, // no_local
							   1, // no_ack
							   0, // exclusive
							   amqp_empty_table);

			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Amqp reconnect successful- connected\n");
			continue;
	}

		amqp_maybe_release_buffers(profile->conn_active->state);

		result = amqp_consume_message(profile->conn_active->state, &envelope, &timeout, 0);

		if (result.reply_type == AMQP_RESPONSE_NORMAL) {
				switch_malloc(message, sizeof(char) * envelope.message.body.len + 1);
				memcpy(message, envelope.message.body.bytes, envelope.message.body.len);
				message[envelope.message.body.len] = '\0';

			json = cJSON_Parse(message);
			if (json) {
				cJSON *response = cJSON_GetObjectItem(json, "response");
				cJSON *uuid = cJSON_GetObjectItem(json, "Fetch-UUID");
				if (response && response->valuestring && uuid && uuid->valuestring) {
					char *xml_str = strdup(response->valuestring); // Make a copy
					char *uuid_str = uuid->valuestring;

					switch_mutex_lock(profile->replies_mutex);
					reply = profile->replies;
					while (reply != NULL) {
						if (!strncmp(reply->uuid_str, uuid_str, SWITCH_UUID_FORMATTED_LENGTH)) {
							if (!reply->xml_str) {
								reply->xml_str = xml_str; // Store our copy
								switch_thread_cond_broadcast(profile->new_reply);
								status = SWITCH_STATUS_SUCCESS;
							} else {
								free(xml_str); // Already have a response, free our copy
							}
							break;
						}
						reply = reply->next;
					}
					if (!reply) {
						free(xml_str); // No matching request found, free our copy
					}
					switch_mutex_unlock(profile->replies_mutex);
				}
				cJSON_Delete(json);
			}

			switch_safe_free(message);
			amqp_destroy_envelope(&envelope);
	}
	}
	amqp_bytes_free(queuename);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Fetch XML receiver thread stopped\n");
	switch_thread_exit(thread, SWITCH_STATUS_SUCCESS);
	return NULL;
}

switch_status_t mod_amqp_fetch_xml_send(mod_amqp_xml_fetch_profile_t *profile, mod_amqp_message_t *msg)
{
	amqp_table_entry_t messageTableEntries[2];
	amqp_basic_properties_t props;
	int status;
	uint64_t timestamp;

	if (!profile->conn_active || !profile->conn_active->state) {
		/* No connection, so we can not send the message. */
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Profile[%s] not active\n", profile->name);
		return SWITCH_STATUS_NOT_INITALIZED;
	}

	memset(&props, 0, sizeof(amqp_basic_properties_t));

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
	props.content_type = amqp_cstring_bytes(profile->content_type);

	if (profile->delivery_mode > 0) {
		props._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
		props.delivery_mode = profile->delivery_mode;
	}

	if (profile->delivery_timestamp) {
		props._flags |= AMQP_BASIC_TIMESTAMP_FLAG | AMQP_BASIC_HEADERS_FLAG;
		props.timestamp = (uint64_t)time(NULL);
		props.headers.num_entries = 1;
		props.headers.entries = messageTableEntries;
		timestamp = (uint64_t)switch_micro_time_now();
		messageTableEntries[0].key = amqp_cstring_bytes("x_Liquid_MessageSentTimeStamp");
		messageTableEntries[0].value.kind = AMQP_FIELD_KIND_TIMESTAMP;
		messageTableEntries[0].value.value.u64 = (uint64_t)(timestamp / 1000000);
		messageTableEntries[1].key = amqp_cstring_bytes("x_Liquid_MessageSentTimeStampMicro");
		messageTableEntries[1].value.kind = AMQP_FIELD_KIND_U64;
		messageTableEntries[1].value.value.u64 = timestamp;
	}

	status = amqp_basic_publish(profile->conn_active->state, 1, amqp_cstring_bytes(profile->exchange),
								amqp_cstring_bytes(msg->routing_key), 0, 0, &props, amqp_cstring_bytes(msg->pjson));

	if (status < 0) {
		const char *errstr = amqp_error_string2(-status);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT,
						  "Profile[%s] failed to send directory request on connection[%s]: %s\n", profile->name,
						  profile->conn_active->name, errstr);

		/* This is bad, we couldn't send the message. Clear up any connection */
		mod_amqp_connection_close(profile->conn_active);
		profile->conn_active = NULL;
		return SWITCH_STATUS_SOCKERR;
	}

	return SWITCH_STATUS_SUCCESS;
}
