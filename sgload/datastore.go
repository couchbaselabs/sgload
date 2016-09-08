package sgload

type DataStore interface {

	// Creates a new user in the data store
	CreateUser(u UserCred) error

	// Creates a document in the data store
	CreateDocument(d Document) error

	// Sets the user credentials to use for all subsequent requests
	SetUserCreds(u UserCred)
}

type UserCred struct {
	Username string `json:"username"` // Username part of basicauth credentials for this writer to use
	Password string `json:"password"` // Password part of basicauth credentials for this writer to use
}

func (u UserCred) Empty() bool {
	return u.Username == "" && u.Password == ""
}

type Document map[string]interface{}
