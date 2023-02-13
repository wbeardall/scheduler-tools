from schedtools.utils import walltime_to

class PBSJob(dict):
    # TODO: Make into a proper dataclass if needed.

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
        if hasattr(self, "resources_used.walltime"):
            return 100 * walltime_to(self["resources_used.walltime"]) / walltime_to(self["Resource_List.walltime"]) 
        return 0