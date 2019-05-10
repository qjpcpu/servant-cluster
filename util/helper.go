package util

func MasterKey(prefix string) string {
	return prefix + "/master"
}

func ServantKey(prefix string) string {
	return prefix + "/servants"
}
