package file

import "net/url"

// ParseBackupUrl s3://my-bucket/my-dir/my-object.db
func ParseBackupUrl(backupURL string) (string, string, string, error) {
	parse, err := url.Parse(backupURL)
	if err != nil {
		return "", "", "", err
	}
	return parse.Scheme, parse.Host, parse.Path[1:], nil
}
