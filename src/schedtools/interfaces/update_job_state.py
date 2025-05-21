import argparse

from schedtools.schemas import JobState
from schedtools.sql import update_job_state as impl


def update_job_state():
    parser = argparse.ArgumentParser(
        description="Update the state of a job in the tracking database"
    )
    parser.add_argument(
        "--state",
        type=str,
        choices=[state.value for state in JobState],
        help="The new state for the job",
        required=True,
    )
    parser.add_argument(
        "--job-id",
        type=str,
        help="The ID of the job to update. If not provided, will use the JOB_ID environment variable",
    )
    parser.add_argument(
        "--comment",
        type=str,
        help="Optional comment to add to the job",
    )
    parser.add_argument(
        "--on-fail",
        type=str,
        choices=["raise", "warn", "ignore"],
        default="raise",
        help="What to do if the update fails",
    )

    args = parser.parse_args()

    impl(
        state=args.state,
        comment=args.comment,
        job_id=args.job_id,
        on_fail=args.on_fail,
    )


if __name__ == "__main__":
    update_job_state()
