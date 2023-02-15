from schedtools.utils import walltime_to

class PBSJob(dict):
    """Simple dict-like interface for storing PBS job information.
    
    Follows field name conventions of `qstat -f` for simplicity, even though
    these are non-Pythonic.
    """
    # TODO: Make into a proper dataclass if needed.
    def __getattr__(self, key, *args, **kwargs):
        try:
            return super().__getattr__(key, *args, **kwargs)
        except AttributeError:
            return self.__getitem__(key, *args, **kwargs)

    @property
    def id(self):
        return self["id"]
    @property
    def jobscript_path(self):
        if "jobscript_path" in self:
            return self["jobscript_path"]
        return self["Submit_arguments"].replace("\n","").split()[-1]

    @property
    def percent_completion(self):
        if "resources_used.walltime" in self:
            return 100 * walltime_to(self["resources_used.walltime"]) / walltime_to(self["Resource_List.walltime"]) 
        return 0