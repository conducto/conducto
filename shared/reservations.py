import json
import warnings


class PipelineReservation:

    CLASSES = ["2x4", "4x8", "2x8", "4x16", "2x16", "4x32"]

    def __init__(self, reservations=None, timeout=60):
        if reservations is None:
            reservations = {"2x4": 1}
        self.reservations = {
            i: reservations.get(i, 0) for i in PipelineReservation.CLASSES
        }
        self.timeout = timeout

        if self.timeout < 60:
            warnings.warn(
                f"Instances are billed minimally for 60s, setting a timeout of {timeout} can be wasteful"
            )

    def instance_fleet_kwargs(self):
        for cls in PipelineReservation.CLASSES:
            cpu, mem = map(int, cls.split("x"))
            yield (cpu, mem), {
                "cpu": cpu,
                "mem": mem,
                "reserved_count": self.reservations[cls],
                "timeout": self.timeout,
            }

    @property
    def id(self):
        return {"timeout": self.timeout, "reservations": self.reservations}

    @classmethod
    def from_json(cls, d):
        if d is None:
            return PipelineReservation()
        return PipelineReservation(d["reservations"], d["timeout"])


PipelineReservation.DEFAULT = PipelineReservation()
PipelineReservation.DEV = PipelineReservation(timeout=3600)
PipelineReservation.PROD = PipelineReservation(reservations={})


# TODO (apeng)
#  - "aggressive" reservation mode that will scan the pipeline and reserve what is maximally necessary
#  - attempt to pre-pull docker images onto hosts
#  - timeouts by instance class?
#  - mixed instance type reservations, i.e. reserve one really OP instance that will many different sizes
#  - custom size of "catch all" instance type (i.e. make 4x8 the catch all instead of 2x4)
