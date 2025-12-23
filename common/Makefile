gen:
	@echo "Generating Go code from Proto files..."
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/geo/geo.proto \
	       proto/ride/ride.proto \
	       proto/driver/driver.proto \
	       proto/passenger/passenger.proto
	@echo "Done!"
