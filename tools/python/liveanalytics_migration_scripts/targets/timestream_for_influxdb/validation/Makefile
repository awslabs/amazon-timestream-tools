.PHONY: all build validate 
all: build validate 
build: 
	docker build . -t validator 
validate: 
	docker run --rm \
		-v $(PWD):/app \
		-v $(HOME)/.aws:/root/.aws:ro \
		-e AWS_PROFILE=default \
		-it \
		validator \
		bash -c "python validator.py"
influx_only: 
	docker run --rm \
		-v $(PWD):/app \
		-v $(HOME)/.aws:/root/.aws:ro \
		-e AWS_PROFILE=default \
		-it \
		validator \
		bash -c "python validator.py --influx-only"
skip_wal: 
	docker run --rm \
		-v $(PWD):/app \
		-v $(HOME)/.aws:/root/.aws:ro \
		-e AWS_PROFILE=default \
		-it \
		validator \
		bash -c "python validator.py --skip-wal-check"
track_influx: 
	docker run --rm \
		-v $(PWD):/app \
		-v $(HOME)/.aws:/root/.aws:ro \
		-e AWS_PROFILE=default \
		-it \
		validator \
		bash -c "python validator.py --influx-only --skip-wal-check"
