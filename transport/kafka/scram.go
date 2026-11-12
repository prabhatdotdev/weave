package kafka

import "github.com/xdg-go/scram"

type scramClient struct {
	*scram.ClientConversation
	hash scram.HashGeneratorFcn
}

func (c *scramClient) Begin(username, password, authzID string) error {
	client, err := c.hash.NewClient(username, password, authzID)
	if err != nil {
		return err
	}
	c.ClientConversation = client.NewConversation()
	return nil
}
