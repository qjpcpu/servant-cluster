package util

const (
	ModuleName = "servant-cluster"
)

func MasterKey(prefix string) string {
	return prefix + "/master"
}

func ServantKey(prefix string) string {
	return prefix + "/servants"
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
