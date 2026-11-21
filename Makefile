PYTHON ?= $(shell command -v python3.12 2>/dev/null || command -v python3.11 2>/dev/null || command -v python3)
PY ?= .logistic-map/.venv/bin/python
SAMPLE ?= go
EVAL_PY ?= eval/.venv/bin/python

.PHONY: help setup test samples samples-list sample sample-no-open index index-links eval clean

help:
	@printf '%s\n' \
	  'make setup          create .logistic-map/.venv with LadybugDB' \
	  'make test           run the deterministic CLI test suite' \
	  'make sample         build and open a sample map (SAMPLE=go)' \
	  'make sample-no-open build a sample map without opening it' \
	  'make samples        regenerate test/sample from eval logs' \
	  'make samples-list   list the available samples' \
	  'make index          build every sample graph and open the index' \
	  'make index-links    refresh the index without rendering graphs' \
	  'make eval           run the Inspect evaluation' \
	  'make clean          remove generated test artefacts'

setup:
	PYTHON="$(PYTHON)" bash scripts/bootstrap.sh --no-init

$(PY):
	PYTHON="$(PYTHON)" bash scripts/bootstrap.sh --no-init

test: $(PY)
	$(PY) test/run_tests.py -v

samples:
	@test -x "$(EVAL_PY)" || { echo "eval environment missing; run ./eval/bootstrap.sh"; exit 1; }
	$(EVAL_PY) test/scripts/extract_samples.py
	@$(PY) test/scripts/build_index.py --no-build >/dev/null

samples-list: $(PY)
	$(PY) test/scripts/open_sample.py --list

index: $(PY)
	$(PY) test/scripts/build_index.py --force
	@open test/.build/index.html

index-links: $(PY)
	$(PY) test/scripts/build_index.py --no-build

sample: $(PY)
	$(PY) test/scripts/open_sample.py --sample $(SAMPLE)
	@$(PY) test/scripts/build_index.py --no-build >/dev/null

sample-no-open: $(PY)
	$(PY) test/scripts/open_sample.py --sample $(SAMPLE) --no-open
	@$(PY) test/scripts/build_index.py --no-build >/dev/null

eval:
	./eval/run.sh

clean:
	rm -rf test/.build
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
