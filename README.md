A collectd plugin for IBM GPFS

Plugin definition: 
> <LoadPlugin python>
>     Globals true
> </LoadPlugin>
> 
> <Plugin python>
>     ModulePath "/var/lib/collectd/plugins/python"
>     Import "collectd_gpfs"
> 
>     <Module collectd_gpfs>
>         PluginName collectd_gpfs
>         Verbose True
>     </Module>
> </Plugin>

Or use standalone mode:
```bash
$ ./collectd_gpfs.py
```


Author:
Guan Ji Chen @ vaneCloud.com
