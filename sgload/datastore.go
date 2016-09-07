package sgload

type DataStore interface {
	CreateUser(u UserCred) error
	CreateDocument(d Document) error
}

type UserCred struct {
	Username string `json:"username"` // Username part of basicauth credentials for this writer to use
	Password string `json:"password"` // Password part of basicauth credentials for this writer to use
}

type Document map[string]interface{}
