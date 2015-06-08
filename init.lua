local framework = require('framework')
local fs = require('fs')
local json = require('json')
local url = require('url')
local table = require('table')
local Plugin  = framework.Plugin
local WebRequestDataSource = framework.WebRequestDataSource
local Accumulator = framework.Accumulator
local auth = framework.util.auth
local gsplit = framework.string.gsplit
local notEmpty = framework.string.notEmpty
local pack = framework.util.pack

local params = framework.params or {}

local options = url.parse(notEmpty(params.statisticsUri, 'http://127.0.0.1:8098/stats'))
options.wait_for_end = true
local ds = WebRequestDataSource:new(options)
local acc = Accumulator:new()
local plugin = Plugin:new(params, ds)

local function parseJson(body)
    local parsed
    pcall(function () parsed = json.parse(body) end)
    return parsed 
end

function plugin:onParseValues(data)
  local metrics = {}

  local stats = parseJson(data)
  if stats then
    metrics['RIAK_RINGS_RECONCILED'] = stats['rings_reconciled']
    metrics['RIAK_GOSSIP_RECEIVED'] = stats['gossip_received']
    metrics['RIAK_CONVERGE_DELAY_MEAN'] = stats['converge_delay_mean']
    metrics['RIAK_REBALANCE_DELAY_MEAN'] = stats['rebalance_delay_mean']
    metrics['RIAK_KV_VNODES_RUNNING'] = stats['riak_kv_vnodes_running']
    metrics['RIAK_KV_VNODEQ_MEDIAN'] = stats['riak_kv_vnodeq_median']
    metrics['RIAK_KV_VNODEQ_TOTAL'] = stats['riak_kv_vnodeq_total']
    metrics['RIAK_PIPE_VNODES_RUNNING'] = stats['riak_pipe_vnodes_running']
    metrics['RIAK_PIPE_VNODEQ_MEDIAN'] = stats['riak_pipe_vnodeq_median']
    metrics['RIAK_PIPE_VNODEQ_TOTAL'] = stats['riak_pipe_vnodeq_total']
    metrics['RIAK_NODE_GET_FSM_IN_RATE'] = stats['node_get_fsm_in_rate']
    metrics['RIAK_NODE_GET_FSM_OUT_RATE'] = stats['node_get_fsm_out_rate']
    metrics['RIAK_NODE_PUT_FSM_IN_RATE'] = stats['node_put_fsm_in_rate']
    metrics['RIAK_NODE_PUT_FSM_OUT_RATE'] = stats['node_put_fsm_out_rate']
    metrics['RIAK_NODE_GETS'] = stats['node_gets']
    metrics['RIAK_NODE_PUTS'] = stats['node_puts']
    metrics['RIAK_PBC_CONNECTS'] = stats['pbc_connects']
    metrics['RIAK_READ_REPAIRS'] = stats['read_repairs']
    metrics['RIAK_NODE_GET_FSM_TIME_MEAN'] = stats['node_get_fsm_time_mean']
    metrics['RIAK_NODE_PUT_FSM_TIME_MEAN'] = stats['node_put_fsm_time_mean']
    metrics['RIAK_NODE_GET_FSM_SIBLINGS_MEAN'] = stats['node_get_fsm_siblings_mean']
    metrics['RIAK_NODE_GET_FSM_OBJSIZE_MEAN'] = stats['node_get_fsm_objsize_mean']
    metrics['RIAK_MEMORY_TOTAL'] = stats['memory_total']
    metrics['RIAK_PIPELINE_ACTIVE'] = stats['pipeline_active']
    metrics['RIAK_PIPELINE_CREATE_ONE'] = stats['pipeline_create_one']
    metrics['RIAK_SEARCH_VNODEQ_MEAN'] = stats['riak_search_vnodeq_mean']
    metrics['RIAK_SEARCH_VNODEQ_TOTAL'] = stats['riak_search_vnodeq_total']
    metrics['RIAK_SEARCH_VNODES_RUNNING'] = stats['riak_search_vnodes_running']
    metrics['RIAK_CONSISTENT_GETS'] = stats['consistent_gets']
    metrics['RIAK_CONSISTENT_GET_OBJSIZE_MEAN'] = stats['consistent_get_objsize_mean']
    metrics['RIAK_CONSISTENT_GET_TIME_MEAN'] = stats['consistent_get_time_mean']
    metrics['RIAK_CONSISTENT_PUTS'] = stats['consistent_puts']
    metrics['RIAK_CONSISTENT_PUT_OBJSIZE_MEAN'] = stats['consistent_put_objsize_mean']
    metrics['RIAK_CONSISTENT_PUT_TIME_MEAN'] = stats['consistent_put_time_mean']
  end

  return metrics 
end

plugin:run()
