local cjson = require "cjson"
local sandbox = require "kong.tools.sandbox".sandbox


local kong = kong
local ngx = ngx
local timer_at = ngx.timer.at


local sandbox_opts = { env = { kong = kong, ngx = ngx } }


local function log(premature, conf, message)
  if premature then
    return
  end

  local host = conf.host
  local port = conf.port
  local timeout = conf.timeout
  local keepalive = conf.keepalive

  local sock = ngx.socket.tcp()
  sock:settimeout(timeout)

  local ok, err = sock:connect(host, port)
  if not ok then
    kong.log.err("failed to connect to ", host, ":", tostring(port), ": ", err)
    return
  end

  if conf.tls then
    ok, err = sock:sslhandshake(true, conf.tls_sni, false)
    if not ok then
      kong.log.err("failed to perform TLS handshake to ", host, ":", port, ": ", err)
      return
    end
  end

  ok, err = sock:send(cjson.encode(message) .. "\n")
  if not ok then
    kong.log.err("failed to send data to ", host, ":", tostring(port), ": ", err)
  end

  ok, err = sock:setkeepalive(keepalive)
  if not ok then
    kong.log.err("failed to keepalive to ", host, ":", tostring(port), ": ", err)
    return
  end
end


local TcpLogHandler = {
  PRIORITY = 7,
  VERSION = "2.1.0",
}

function TcpLogHandler:access(conf)
  if conf.log_body then
    local body, err = kong.request.get_raw_body()
    if err then
      kong.log.err(err)
      kong.ctx.plugin.request_body = ""
    else
      kong.ctx.plugin.request_body = body
    end
    kong.ctx.plugin.response_body = {}
  end
end


function TcpLogHandler:body_filter(conf)
  if conf.log_body then
    local chunk = ngx.arg[1]
    local body = kong.ctx.plugin.response_body
    body[#body + 1] = chunk
    kong.ctx.plugin.response_body=body
  end
end

function TcpLogHandler:log(conf)
  if conf.custom_fields_by_lua then
    local set_serialize_value = kong.log.set_serialize_value
    for key, expression in pairs(conf.custom_fields_by_lua) do
      set_serialize_value(key, sandbox(expression, sandbox_opts)())
    end
  end

  if conf.log_body then
    local set_serialize_value = kong.log.set_serialize_value
    set_serialize_value("req_body",kong.ctx.plugin.request_body)
    set_serialize_value("res_body",kong.ctx.plugin.response_body)
  end

  local message = kong.log.serialize()
  local ok, err = timer_at(0, log, conf, message)
  if not ok then
    kong.log.err("failed to create timer: ", err)
  end
end


return TcpLogHandler
