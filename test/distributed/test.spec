{node, n1, 'n1@127.0.0.1'}.
{node, n2, 'n2@127.0.0.1'}.

{init, [n1], [{node_start, [
    {monitor_master, true},
    {username,"roman"},
    {password,"system"},
    {erl_flags,"-pa /home/roman/PROJECTS/SOURCE/dlss/_build/default/lib/*/ebin"},
    {startup_functions, [{dlss_backend,init_backend,[ ]}]}
]}]}.

{init, [n2], [{node_start, [
    {monitor_master, true},
    {username,"roman"},
    {password,"system"},
    {erl_flags,"-pa /home/roman/PROJECTS/SOURCE/dlss/_build/default/lib/*/ebin"},
    {startup_functions, [{dlss_backend,init_backend,[ ] }]}
]}]}.

{logdir, all_nodes, "../../_build/test/logs/"}.
{logdir, master, "../../_build/test/logs/"}.

{define, 'DISTRIBUTED_TEST', "../"}.

{suites,[ n1 ], 'DISTRIBUTED_TEST', [
    dlss_segment_SUITE
]}.