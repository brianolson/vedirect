# Go tools for VE.Direct

```sh
go get github.com/brianolson/vedirect
```

```go
import "github.com/brianolson/vedirect"
```

Read serial from Victron Energy devices. Parse and make available as Go objects or JSON. Some utility code for dealing with the records.

See example apps cmd/vedump and cmd/vesend

## vesend

```sh
vesend -dev /dev/serial/by-id/usb-VictronEnergy_BV_VE_Direct_cable_*-port0  -post 'https://127.0.0.1:1234/my_post_receiver' -send-period 1000 -z -serve :8080
```

```sh
curl http://127.0.0.1:8080/records.json
```

