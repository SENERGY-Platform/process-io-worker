package docker

import (
	"context"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func Mqtt(ctx context.Context, wg *sync.WaitGroup) (hostport string, containerip string, err error) {
	log.Println("start mqtt")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "eclipse-mosquitto:1.6.12",
			ExposedPorts: []string{"1883/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("1883/tcp"),
				wait.ForNop(waitretry(time.Minute, func(ctx context.Context, target wait.StrategyTarget) error {
					log.Println("try to connection to broker...")
					p, err := target.MappedPort(ctx, "1883/tcp")
					if err != nil {
						return err
					}
					options := paho.NewClientOptions().
						SetAutoReconnect(true).
						SetCleanSession(false).
						SetClientID("try-test-connection-" + strconv.Itoa(rand.Int())).
						AddBroker("tcp://localhost:" + p.Port())

					client := paho.NewClient(options)
					if token := client.Connect(); token.Wait() && token.Error() != nil {
						log.Println("Error on Mqtt.Connect(): ", token.Error())
						return token.Error()
					}
					defer client.Disconnect(0)
					return nil
				})),
			),
		},
		Started: true,
	})
	if err != nil {
		return "", "", err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container mongo", c.Terminate(context.Background()))
	}()

	containerip, err = c.ContainerIP(ctx)
	if err != nil {
		return "", "", err
	}
	temp, err := c.MappedPort(ctx, "1883/tcp")
	if err != nil {
		return "", "", err
	}
	hostport = temp.Port()

	return hostport, containerip, err
}
