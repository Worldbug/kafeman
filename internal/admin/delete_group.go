package admin

func (a *Admin) DeleteGroup(group string) error {
	return a.getSaramaAdmin().DeleteConsumerGroup(group)
}
