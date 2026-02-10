from collections import defaultdict

class AssayHandler:
    """
    Object that will contain all the information pertaining to one and only
    one assay i.e. samples, assay config information
    """

    def __init__(self, config):
        """
        Initialise the various variables needed for the smooth running of
        the script

        Parameters
        ----------
        config : dict
            Dict containing information for an assay
        """

        self.config = config
        self.assay_code = config.get("assay_code")
        self.assay = config.get("assay")
        self.version = config.get("version")
        self.samples = []
        self.job_info_per_sample = {}
        self.job_info_per_run = {}
        self.job_outputs = {}
        self.jobs = []
        self.missing_output_samples = []
        self.job_summary = defaultdict(lambda: defaultdict(dict))
