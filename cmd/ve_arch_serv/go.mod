module github.com/brianolson/vedirect/cmd/ve_arch_serv

go 1.19

require (
	github.com/brianolson/vedirect v0.0.1
	github.com/fsnotify/fsnotify v1.6.0
)

require (
	github.com/tarm/serial v0.0.0-20180830185346-98f6abe2eb07 // indirect
	golang.org/x/sys v0.1.0 // indirect
)

replace github.com/brianolson/vedirect => ../../../vedirect
replace github.com/brianolson/vedirect/veplot => ../../../vedirect/veplot
