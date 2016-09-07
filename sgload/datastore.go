package sgload

type DataStore interface {
	CreateUser(u UserCred) error
	CreateDocument(d Document) error
}
