package patchain

var (
	// UseDBOptionName reprents the name of the UseDBOption object
	UseDBOptionName = "string"
)

// UseDBOption contains options to modify a database connection
type UseDBOption struct {
	DB     DB
	Finish bool
}

// GetName returns the option's name
func (t *UseDBOption) GetName() string {
	return UseDBOptionName
}

// GetValue returns the database connection
func (t *UseDBOption) GetValue() interface{} {
	return t.DB
}
