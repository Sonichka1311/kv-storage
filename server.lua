#!/usr/bin/env tarantool

box.cfg {
    listen = 3301;
    log_format='json';
    log='logFile.txt';
 }

log = require('log')

box.once(
    "init", function()
        box.schema.space.create('kv_storage')
        box.space.kv_storage:format({
            { name = 'key', type = 'string' },
            { name = 'value', type = 'string' }
        })
        box.space.kv_storage:create_index(
            'primary',
            {
                type = 'hash',
                parts = {'key'}
            })
    end)

local function keyGen(path)
    local key = string.reverse(path)
    local i = string.find(key, '/')
    -- if path contains / at the end
    if (i == 1) then
        i = string.find(key, '/', 2)
        key = string.sub(key, 2, i - 1)
    else
        key = string.sub(key, 1, i - 1)
    end
    key = string.reverse(key)
    return key
end

local function handler(req)
    local method = req.method
    log.info('Method: '..method)
    if method == 'POST' then
        local body = req:json()
        local key = body.key
        local value = body.value
        if (key == nil or value == nil) then
            log.info('Status: 400')
            log.error('Body isn\'t correct')
            return { status = 400 }
        end
        local exists = box.space.kv_storage:count(key)
        -- return code 409 if key already exists, insert if not
        if exists > 0 then
            log.info('Status: 409')
            log.error('Key already exists in database')
            return { status = 409 }
        else
            box.space.kv_storage:insert{key, value}
            log.info('Status: 200')
            log.info('Inserted: { key: '..key..', value: '..value..' }')
            return { status = 200 }
        end
    elseif method == 'PUT' then
        local body = req:json()
        local key = keyGen(req.path)
        local value = body.value
        -- return status 400 if body is incorrect (has no value)
        if (value == nil) then
            log.info('Status: 400')
            log.error('Body isn\'t correct')
            return { status = 400 }
        end
        local exists = box.space.kv_storage:count(key)
        -- update value if key exists, return status 404 if not
        if exists > 0 then
            box.space.kv_storage:put{key, value}
            log.info('Status: 200')
            log.info('Put: { key: '..key..', value: '..value..' }')
            return { status = 200 }
        else
            log.info('Status: 404')
            log.error('Key doesn\'t exists in database')
            return { status = 404 }
        end
    elseif method == 'GET' then
        local key = keyGen(req.path)
        local exists = box.space.kv_storage:count(key)
        -- get value if key exists, return status 404 if not
        if exists > 0 then
            local value = box.space.kv_storage:get{key}['value']
            log.info('Status: 200')
            log.info('Get: { key: '..key..', value: '..value..' }')
            return {
                status = 200;
                body = '{ \"value\": '..value..' }';
            }
        else
            log.info('Status: 404')
            log.error('Key doesn\'t exists in database')
            return { status = 404 }
        end
    elseif method == 'DELETE' then
        local key = keyGen(req.path)
        local exists = box.space.kv_storage:count(key)
        -- delete tuple if key exists, return status 404 if not
        if exists > 0 then
            log.info('Status: 200')
            log.info('Delete: { key: '..key..' }')
            box.space.kv_storage:delete(key)
            return { status = 200 }
        else
            log.info('Status: 404')
            log.error('Key doesn\'t exists in database')
            return { status = 404 }
        end
    else
        log.error('No matching methods')
        return { status = 404 }
    end
end

local server = require('http.server').new(nil, 8080)

server:route({ path = '/kv', method = 'POST' }, handler) -- for POST method
server:route({ path = 'kv/:key' }, handler) -- for PUT, GET and DELETE methods

server:start()
