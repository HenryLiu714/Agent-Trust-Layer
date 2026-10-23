# Safety tests

The rules irimi keeps on every run, and the test that fails when one is broken. AGENTS.md lists
the same rules; a change that needs one of these tests edited is a change to the rule.

| Rule | Test | What breaks if it fails |
|---|---|---|
| `mitmproxy` is imported only in `src/irimi/engine/mitm.py` | `test_import_boundary.py::test_mitmproxy_is_imported_only_in_engine_mitm` | The decision, the store and the reports can no longer be tested, or the engine swapped, without a proxy. |
| A module imports only from lower layers, and every module has a layer | `test_import_boundary.py::test_no_module_imports_a_later_module_or_layer`, `::test_every_module_is_assigned_a_layer`, `::test_the_architecture_docs_place_the_same_packages_in_the_same_layers` | Import cycles, and a layer table in `docs/architecture.md` that no longer describes the code. |
| No test module imports another; shared helpers live in `tests/support/` | `test_import_boundary.py::test_no_test_module_imports_another` | Splitting or renaming one test file breaks unrelated ones. |
| Nothing in a mitmproxy hook raises | `test_hooks_never_raise.py` | A raised request hook makes mitmproxy forward the flow, so a write reaches the real service and nothing records it. |
| The scope rule: no classification that forwards live applies to an unsafe method it did not name | `test_scope_rule.py` | A `DELETE` or `PUT` classified as a read, telemetry or an LLM call is performed for real. |
| Every response irimi decided carries `Irimi-Answered-By`; a live forward carries none | `test_answered_by.py` | An agent, or a person reading a trace, cannot tell an answer irimi made up from one the service sent. |
| The write log holds only writes that would have happened | `test_write_log.py` | Later reads show the agent a refund that was refused, or one refund twice, and the summary lists webhooks for writes that never happened. |

The checkers these tests share (`stamping_violations`, `write_log_violations` and the rest) are in
`tests/support/invariants.py`. Each returns what breaks a rule, one string per break, and each
test asserts it empty on a real run and non-empty on a forged one, so none of them can pass
without checking anything.
