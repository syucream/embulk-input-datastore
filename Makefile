build:
	gradle build

embulk_run:
	gradle embulk_run

publish_gem: build
	gradle classpath
	gradle gemPush

.PHONY: start_datastore_emulator
start_datastore_emulator:
	gcloud beta emulators datastore start --no-store-on-disk --quiet &
	sleep 1
	$(gcloud beta emulators datastore env-init)

.PHONY: stop_datastore_emulator
stop_datastore_emulator:
	gcloud beta emulators datastore env-unset
	ps aux | grep CloudDatastore | grep -v grep | awk '{ print "kill " $2 }' | sh
