package rdpgpg

import (
	"github.com/starkandwayne/rdpgd/globals"

	_ "github.com/lib/pq"
	"github.com/starkandwayne/rdpgd/pg"
)

func GetList(address string, sq string) (list []string, err error) {
	p := pg.NewPG(address, globals.PBPort, `rdpg`, `rdpg`, globals.PGPass)
	db, err := p.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows := []string{}
	err = db.Select(&rows, sq)
	if err != nil {
		return nil, err
	}
	return rows, nil

}

//Consider merging this with the version in admin/db.go. Would need to add
// row count variable and adjust calls to this function accordingly.
func ExecQuery(address string, sq string) (err error) {
	p := pg.NewPG(address, globals.PBPort, `rdpg`, `rdpg`, globals.PGPass)
	db, err := p.Connect()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(sq)
	return err
}
