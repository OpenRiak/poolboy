# since we are (principally) a dependency app of riak, we usually are
# built by the top-level rebar3, of a specific version, so let's find
# it in the usual location.
REBAR ?= ../riak/rebar3

DIALYZER_WARNINGS = -Wunmatched_returns -Werror_handling \
                    -Wrace_conditions -Wunderspecs

.PHONY: all compile test clean get-deps build-plt dialyze

all: compile

compile:
	@$(REBAR) compile

test: compile
	@$(REBAR) do eunit skip_deps=true, cover

qc: compile
	@$(REBAR) qc skip_deps=true

clean:
	@$(REBAR) clean

get-deps:
	@$(REBAR) get-deps

dialyze: compile
	@$(REBAR) dialyzer
