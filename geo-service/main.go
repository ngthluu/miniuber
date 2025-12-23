package main

import (
	"context"
	"log"
	"net"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	pb "github.com/ngthluu/miniuber/common/proto/geo"
)

type server struct {
	pb.UnimplementedGeoServiceServer
	rdb *redis.Client
}

const (
	driverLocationKey = "drivers:locations"
)

func (s *server) UpdateDriverLocation(ctx context.Context, req *pb.UpdateDriverLocationRequest) (*pb.UpdateDriverLocationResponse, error) {
	err := s.rdb.GeoAdd(ctx, driverLocationKey, &redis.GeoLocation{
		Name:      req.DriverId,
		Longitude: req.Longitude,
		Latitude:  req.Latitude,
	}).Err()

	if err != nil {
		log.Printf("Failed to update location for %s: %v", req.DriverId, err)
		return &pb.UpdateDriverLocationResponse{Success: false}, err
	}

	log.Printf("Updated location: Driver %s at [%f, %f]", req.DriverId, req.Latitude, req.Longitude)
	return &pb.UpdateDriverLocationResponse{Success: true}, nil
}

func (s *server) GetNearbyDrivers(ctx context.Context, req *pb.GetNearbyDriversRequest) (*pb.GetNearbyDriversResponse, error) {
	locations, err := s.rdb.GeoSearch(ctx, driverLocationKey, &redis.GeoSearchQuery{
		Longitude:  req.Longitude,
		Latitude:   req.Latitude,
		Radius:     float64(req.RadiusKm),
		RadiusUnit: "km",
		Sort:       "ASC", // Closest first
		Count:      int(req.Limit),
	}).Result()

	if err != nil {
		return nil, err
	}

	var driverIDs []string
	for _, loc := range locations {
		driverIDs = append(driverIDs, loc.Name)
	}

	return &pb.GetNearbyDriversResponse{DriverIds: driverIDs}, nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Test Redis connection
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	// Listen on TCP port
	lis, err := net.Listen("tcp", ":50052") // Port 50052 for Geo Service
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 3. Start gRPC Server
	s := grpc.NewServer()
	pb.RegisterGeoServiceServer(s, &server{rdb: rdb})

	log.Printf("Geo Service listening on port 50052...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
