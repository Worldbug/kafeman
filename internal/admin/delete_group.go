package admin

func (a *Admin) DeleteGroup(group string) error {
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return err
	}

	return adm.DeleteConsumerGroup(group)
}
