package cfsb

type Service struct {
	ServiceID   string         `db:"service_id" json:"id"`
	Name        string         `db:"name" json:"name"`
	Description string         `db:"description" json:"description"`
	Bindable    bool           `db:"bindable" json:"bindable"`
	Tags        []string       `json:"tags"`
	Metadata    ServiceDetails `json:"metadata"`
	Plans       []*Plan        `json:"plans"`
	//DashboardClient   DashboardClient `json:"dashboard_client,omitempty"`
}

type ServiceDetails struct {
	Label       string      `db:"label" json:"label"`
	Description string      `db:"description" json:"description"`
	Provider    string      `db:"provider" json:"provider"`
	Version     string      `db:"version" json:"version"`
	Requires    []string    `json:"requires"`
	Tags        []string    `json:"tags"`
	Metadata    TileDetails `json:"metadata"`
}

type TileDetails struct {
	DisplayName         string `db:"displayname" json:"displayname"`
	ImageUrl            string `db:"imageurl" json:"imageurl"`
	LongDescription     string `db:"longdescription" json:"longdescription"`
	ProviderDisplayName string `db:"provider" json:"providerdisplayname"`
	DocumentationUrl    string `db:"documentationurl" json:"documentationurl"`
	SupportUrl          string `db:"supporturl" json:"supporturl"`
}

type DashboardClient struct {
	ClientID     string `json:"id,omitempty"`
	ClientSecret string `json:"secret,omitempty"`
	RedirectURI  string `json:"redirect_uri,omitempty"`
}
