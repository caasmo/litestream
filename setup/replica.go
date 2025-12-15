package setup

import (
	"fmt"
	"strings"

	"net/url"
	"path"
	
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/oss"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/benbjohnson/litestream/webdav"

	"github.com/benbjohnson/litestream/config"
)

type boolSetting struct {
	value bool
	set   bool
}

func newBoolSetting(defaultValue bool) boolSetting {
	return boolSetting{value: defaultValue}
}

func (s *boolSetting) Set(value bool) {
	s.value = value
	s.set = true
}

func (s *boolSetting) ApplyDefault(value bool) {
	if !s.set {
		s.value = value
	}
}


// NewReplicaFromConfig instantiates a replica for a DB based on a config.
func NewReplicaFromConfig(c *config.ReplicaConfig, db *litestream.DB) (_ *litestream.Replica, err error) {
	// Ensure user did not specify URL in path.
	if litestream.IsURL(c.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", c.Path)
	}

	// Reject age encryption configuration as it's currently non-functional.
	// Age encryption support was removed during the LTX storage layer refactor
	// and has not been reimplemented. Accepting this config would silently
	// write plaintext data to remote storage instead of encrypted data.
	// See: https://github.com/benbjohnson/litestream/issues/790
	if len(c.Age.Identities) > 0 || len(c.Age.Recipients) > 0 {
		return nil, fmt.Errorf("age encryption is not currently supported, if you need encryption please revert back to Litestream v0.3.x")
	}

	// Build replica.
	r := litestream.NewReplica(db)
	if v := c.SyncInterval; v != nil {
		r.SyncInterval = *v
	}

	// Build and set client on replica.
	switch c.ReplicaType() {
	case "file":
		if r.Client, err = newFileReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "s3":
		if r.Client, err = NewS3ReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "gs":
		if r.Client, err = newGSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "abs":
		if r.Client, err = newABSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "sftp":
		if r.Client, err = newSFTPReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "webdav":
		if r.Client, err = newWebDAVReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "nats":
		if r.Client, err = newNATSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "oss":
		if r.Client, err = newOSSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", c.Type)
	}

	return r, nil
}

// newFileReplicaClientFromConfig returns a new instance of file.ReplicaClient built from config.
func newFileReplicaClientFromConfig(c *config.ReplicaConfig, r *litestream.Replica) (_ *file.ReplicaClient, err error) {
	// Ensure URL & path are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for file replica")
	}

	// Parse configPath from URL, if specified.
	configPath := c.Path
	if c.URL != "" {
		if _, _, configPath, err = litestream.ParseReplicaURL(c.URL); err != nil {
			return nil, err
		}
	}

	// Ensure path is set explicitly or derived from URL field.
	if configPath == "" {
		return nil, fmt.Errorf("file replica path required")
	}

	// Expand home prefix and return absolute path.
	if configPath, err = config.Expand(configPath); err != nil {
		return nil, err
	}

	// Instantiate replica and apply time fields, if set.
	client := file.NewReplicaClient(configPath)
	client.Replica = r
	return client, nil
}

// NewS3ReplicaClientFromConfig returns a new instance of s3.ReplicaClient built from config.
// Exported for testing.
func NewS3ReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *s3.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for s3 replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for s3 replica")
	}

	bucket, configPath := c.Bucket, c.Path
	region, endpoint, skipVerify := c.Region, c.Endpoint, c.SkipVerify
	signSetting := newBoolSetting(false)
	if v := c.SignPayload; v != nil {
		signSetting.Set(*v)
	}
	requireSetting := newBoolSetting(true)
	if v := c.RequireContentMD5; v != nil {
		requireSetting.Set(*v)
	}

	// Use path style if an endpoint is explicitly set. This works because the
	// only service to not use path style is AWS which does not use an endpoint.
	forcePathStyle := (endpoint != "")
	if v := c.ForcePathStyle; v != nil {
		forcePathStyle = *v
	}

	// Apply settings from URL, if specified.
	var (
		endpointWasSet        bool
		usignPayload          bool
		usignPayloadSet       bool
		urequireContentMD5    bool
		urequireContentMD5Set bool
	)
	if endpoint != "" {
		endpointWasSet = true
	}

	if c.URL != "" {
		_, host, upath, query, _, err := litestream.ParseReplicaURLWithQuery(c.URL)
		if err != nil {
			return nil, err
		}

		var (
			ubucket         string
			uregion         string
			uendpoint       string
			uforcePathStyle bool
		)

		if strings.HasPrefix(host, "arn:") {
			ubucket = host
			uregion = litestream.RegionFromS3ARN(host)
		} else {
			ubucket, uregion, uendpoint, uforcePathStyle = s3.ParseHost(host)
		}

		// Override with query parameters if provided
		if qEndpoint := query.Get("endpoint"); qEndpoint != "" {
			// Ensure endpoint has a scheme
			if !strings.HasPrefix(qEndpoint, "http://") && !strings.HasPrefix(qEndpoint, "https://") {
				// Default to http for non-TLS endpoints (common for local/dev)
				qEndpoint = "http://" + qEndpoint
			}
			uendpoint = qEndpoint
			// Default to path style for custom endpoints unless explicitly set to false
			if query.Get("forcePathStyle") != "false" {
				uforcePathStyle = true
			}
			endpointWasSet = true
		}
		if qRegion := query.Get("region"); qRegion != "" {
			uregion = qRegion
		}
		if qForcePathStyle := query.Get("forcePathStyle"); qForcePathStyle != "" {
			uforcePathStyle = qForcePathStyle == "true"
		}
		if qSkipVerify := query.Get("skipVerify"); qSkipVerify != "" {
			skipVerify = qSkipVerify == "true"
		}
		if v, ok := litestream.BoolQueryValue(query, "signPayload", "sign-payload"); ok {
			usignPayload = v
			usignPayloadSet = true
		}
		if v, ok := litestream.BoolQueryValue(query, "requireContentMD5", "require-content-md5"); ok {
			urequireContentMD5 = v
			urequireContentMD5Set = true
		}

		// Only apply URL parts to field that have not been overridden.
		if configPath == "" {
			configPath = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
		if endpoint == "" {
			endpoint = uendpoint
		}
		if !forcePathStyle {
			forcePathStyle = uforcePathStyle
		}
		if !signSetting.set && usignPayloadSet {
			signSetting.Set(usignPayload)
		}
		if !requireSetting.set && urequireContentMD5Set {
			requireSetting.Set(urequireContentMD5)
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica")
	}

	isTigris := litestream.IsTigrisEndpoint(endpoint)
	if !isTigris && !endpointWasSet && litestream.IsTigrisEndpoint(c.Endpoint) {
		isTigris = true
	}

	// Build replica.
	client := s3.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.SecretAccessKey = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = configPath
	client.Region = region
	client.Endpoint = endpoint
	client.ForcePathStyle = forcePathStyle
	client.SkipVerify = skipVerify
	if isTigris {
		signSetting.ApplyDefault(true)
		requireSetting.ApplyDefault(false)
	}

	client.SignPayload = signSetting.value
	client.RequireContentMD5 = requireSetting.value

	// Apply upload configuration if specified.
	if c.PartSize != nil {
		client.PartSize = int64(*c.PartSize)
	}
	if c.Concurrency != nil {
		client.Concurrency = *c.Concurrency
	}

	return client, nil
}

// newGSReplicaClientFromConfig returns a new instance of gs.ReplicaClient built from config.
func newGSReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *gs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for gs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for gs replica")
	}

	bucket, configPath := c.Bucket, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, uhost, upath, err := litestream.ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if configPath == "" {
			configPath = upath
		}
		if bucket == "" {
			bucket = uhost
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for gs replica")
	}

	// Build replica.
	client := gs.NewReplicaClient()
	client.Bucket = bucket
	client.Path = configPath
	return client, nil
}

// newABSReplicaClientFromConfig returns a new instance of abs.ReplicaClient built from config.
func newABSReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *abs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for abs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for abs replica")
	}

	// Build replica.
	client := abs.NewReplicaClient()
	client.AccountName = c.AccountName
	client.AccountKey = c.AccountKey
	client.Bucket = c.Bucket
	client.Path = c.Path
	client.Endpoint = c.Endpoint

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		if client.AccountName == "" && u.User != nil {
			client.AccountName = u.User.Username()
		}
		if client.Bucket == "" {
			client.Bucket = u.Host
		}
		if client.Path == "" {
			client.Path = strings.TrimPrefix(path.Clean(u.Path), "/")
		}
	}

	// Ensure required settings are set.
	if client.Bucket == "" {
		return nil, fmt.Errorf("bucket required for abs replica")
	}

	return client, nil
}

// newSFTPReplicaClientFromConfig returns a new instance of sftp.ReplicaClient built from config.
func newSFTPReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *sftp.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for sftp replica")
	} else if c.URL != "" && c.Host != "" {
		return nil, fmt.Errorf("cannot specify url & host for sftp replica")
	}

	host, user, password, path := c.Host, c.User, c.Password, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if host == "" {
			host = u.Host
		}
		if user == "" && u.User != nil {
			user = u.User.Username()
		}
		if password == "" && u.User != nil {
			password, _ = u.User.Password()
		}
		if path == "" {
			path = u.Path
		}
	}

	// Ensure required settings are set.
	if host == "" {
		return nil, fmt.Errorf("host required for sftp replica")
	} else if user == "" {
		return nil, fmt.Errorf("user required for sftp replica")
	}

	// Build replica.
	client := sftp.NewReplicaClient()
	client.Host = host
	client.User = user
	client.Password = password
	client.Path = path
	client.KeyPath = c.KeyPath
	client.HostKey = c.HostKey

	// Set concurrent writes if specified, otherwise use default (true)
	if c.ConcurrentWrites != nil {
		client.ConcurrentWrites = *c.ConcurrentWrites
	}

	return client, nil
}

// newWebDAVReplicaClientFromConfig returns a new instance of webdav.ReplicaClient built from config.
func newWebDAVReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *webdav.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for webdav replica")
	} else if c.URL != "" && c.WebDAVURL != "" {
		return nil, fmt.Errorf("cannot specify url & webdav-url for webdav replica")
	}

	webdavURL, username, password, path := c.WebDAVURL, c.WebDAVUsername, c.WebDAVPassword, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		// Build WebDAV URL from scheme and host
		scheme := "http"
		if u.Scheme == "webdavs" {
			scheme = "https"
		}
		if webdavURL == "" && u.Host != "" {
			webdavURL = fmt.Sprintf("%s://%s", scheme, u.Host)
		}

		// Extract credentials from URL
		if username == "" && u.User != nil {
			username = u.User.Username()
		}
		if password == "" && u.User != nil {
			password, _ = u.User.Password()
		}
		if path == "" {
			path = u.Path
		}
	}

	// Ensure required settings are set.
	if webdavURL == "" {
		return nil, fmt.Errorf("webdav-url required for webdav replica")
	}

	// Build replica.
	client := webdav.NewReplicaClient()
	client.URL = webdavURL
	client.Username = username
	client.Password = password
	client.Path = path

	return client, nil
}

// newNATSReplicaClientFromConfig returns a new instance of nats.ReplicaClient built from config.
func newNATSReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *nats.ReplicaClient, err error) {
	// Parse URL if provided to extract bucket name and server URL
	var url, bucket string
	if c.URL != "" {
		scheme, host, bucketPath, err := litestream.ParseReplicaURL(c.URL)
		if err != nil {
			return nil, fmt.Errorf("invalid NATS URL: %w", err)
		}
		if scheme != "nats" {
			return nil, fmt.Errorf("invalid scheme for NATS replica: %s", scheme)
		}

		// Reconstruct URL without bucket path
		if host != "" {
			url = fmt.Sprintf("nats://%s", host)
		}

		// Extract bucket name from path
		if bucketPath != "" {
			bucket = strings.Trim(bucketPath, "/")
		}
	}

	// Use bucket from config if not extracted from URL
	if bucket == "" {
		bucket = c.Bucket
	}

	// Ensure required settings are set
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for NATS replica")
	}

	// Validate TLS configuration
	// Both client cert and key must be specified together
	if (c.ClientCert != "") != (c.ClientKey != "") {
		return nil, fmt.Errorf("client-cert and client-key must both be specified for mutual TLS authentication")
	}

	// Build replica client
	client := nats.NewReplicaClient()
	client.URL = url
	client.BucketName = bucket

	// Set authentication options
	client.JWT = c.JWT
	client.Seed = c.Seed
	client.Creds = c.Creds
	client.NKey = c.NKey
	client.Username = c.Username
	client.Password = c.Password
	client.Token = c.Token

	// Set TLS options
	client.RootCAs = c.RootCAs
	client.ClientCert = c.ClientCert
	client.ClientKey = c.ClientKey

	// Set connection options with defaults
	if c.MaxReconnects != nil {
		client.MaxReconnects = *c.MaxReconnects
	}
	if c.ReconnectWait != nil {
		client.ReconnectWait = *c.ReconnectWait
	}
	if c.Timeout != nil {
		client.Timeout = *c.Timeout
	}

	return client, nil
}

// newOSSReplicaClientFromConfig returns a new instance of oss.ReplicaClient built from config.
func newOSSReplicaClientFromConfig(c *config.ReplicaConfig, _ *litestream.Replica) (_ *oss.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for oss replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for oss replica")
	}

	bucket, configPath := c.Bucket, c.Path
	region, endpoint := c.Region, c.Endpoint

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, host, upath, err := litestream.ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}

		var (
			ubucket string
			uregion string
		)

		ubucket, uregion, _ = oss.ParseHost(host)

		// Only apply URL parts to fields that have not been overridden.
		if configPath == "" {
			configPath = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for oss replica")
	}

	// Build replica client.
	client := oss.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.AccessKeySecret = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = configPath
	client.Region = region
	client.Endpoint = endpoint

	// Apply upload configuration if specified.
	if c.PartSize != nil {
		client.PartSize = int64(*c.PartSize)
	}
	if c.Concurrency != nil {
		client.Concurrency = *c.Concurrency
	}

	return client, nil
}
