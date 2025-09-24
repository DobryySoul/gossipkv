package gossipkvserver

import "github.com/redis/go-redis/v9"

func main() {
	redisClient := redis.NewClient(&redis.Options{})
	redisClient.Set()
}
