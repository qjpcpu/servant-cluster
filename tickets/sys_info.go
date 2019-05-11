package tickets

type SysInfo struct {
	Stats []byte
}

type SysInfoGetter func() ([]byte, error)
