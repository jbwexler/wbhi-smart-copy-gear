#!/usr/bin/env python3

import time
import random
import string
import re
import os
import sys
import flywheel_gear_toolkit
import flywheel
import logging
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger(__name__)

WAIT_TIMEOUT = 3600

def get_hdr_fields(dicom, site):
    dicom = dicom.reload()
    if "file-classifier" not in dicom.tags or "header" not in dicom.info:
        return None
    dcm_hdr = dicom.reload().info["header"]["dicom"]

    hdr_fields = {}
    hdr_fields["site"] = site
    hdr_fields["date"] = datetime.strptime(dcm_hdr["AcquisitionDate"], DATE_FORMAT_FW)
    hdr_fields["before_noon"] = float(dcm_hdr["AcquisitionTime"]) < 120000
    if site == 'ucsb':
        hdr_fields["pi_id"], hdr_fields["sub-id"] = re.split('[^0-9a-zA-Z]', dcm_hdr["PatientName"])[:2]
    elif site == 'uci':
        hdr_fields["pi_id"] = re.split('[^0-9a-zA-Z]', dcm_hdr["PatientName"])[0]
        hdr_fields["sub-id"] = re.split('[^0-9a-zA-Z]', dcm_hdr["PatientID"])[0]
    else:
        hdr_fields["pi_id"], hdr_fields["sub-id"] = re.split('[^0-9a-zA-Z]', dcm_hdr["PatientName"])[:2]
    return hdr_fields

def smart_copy(
    src_project,
    group_id: str = None,
    tag: str = None,
    dst_project_label: str = None,
    delete_existing_project = False) -> dict:
    """Smart copy a project to a group and returns API response.

    Args:
        src_project: the source project (mandatory)
        group_id (str): the destination Flywheel group (default: same group as
                           source project)
        session_list (list): list of sessions to be copied
        dst_project_label (str): the destination project label

    Returns:
        dict: copy job response
    """
    
    dst_project_path = os.path.join(group_id, dst_project_label)
    if client.lookup(dst_project_path):
        breakpoint()
        if delete_existing_project:
            delete_project(dst_project_path)
        else:
            dst_project_label = dst_project_label + '_1'

    data = {
        "group_id": group_id,
        "project_label": dst_project_label,
        "filter": {
            "exclude_analysis": False,
            "exclude_notes": False,
            "exclude_tags": True,
            "include_rules": [],
            "exclude_rules": [],
        },
    }

    data["filter"]["include_rules"].append(f"session.tags={tag}")
    print(f'Smart copying "{src_project.label}" to "{group_id}/{dst_project_label}')

    return client.project_copy(src_project.id, data)


def check_smartcopy_job_complete(dst_project) -> bool:
    """Check if a smart copy job is complete.

    Args:
        dst_project (str): the destination project id

    Returns:
        bool: True if the job is complete, False otherwise
    """
    copy_status = dst_project.reload().copy_status
    if copy_status == flywheel.ProjectCopyStatus.COMPLETED:
        return True
    elif copy_status == flywheel.ProjectCopyStatus.FAILED:
        raise RuntimeError(f"Smart copy job to project {dst_project} failed")
    else:
        return False

def check_smartcopy_loop(dst_project: str):
    start_time = time.time()
    while True:
        time.sleep(3)
        if check_smartcopy_job_complete(dst_project):
            log.info(f"Copy project to {dst_project.id} complete")
            return
        if time.time() - start_time > WAIT_TIMEOUT:
            log.error("Wait timeout for copy to complete")
            sys.exit(-1)
            
def mv_to_project(src_project, dst_project):
    print("Moving sessions from {src_project.group.id}/{src_project.project.label} to {dst_project.group.id}/{dst_project.project.label}")
    for session in src_project.sessions.iter():
        try:
            session.update(project=dst_project.id)
        except flywheel.ApiException as exc:
            if exc.status == 422:
                log.error(
                    f"Session {session.subject.label}/{session.label} already exists in {dst_project.label} - Skipping"
                )
            else:
                log.exception(
                    f"Error moving subject {session.subject.label}/{session.label} from {src_project.label} to {dst_project.label}"
                )
        
def check_copied_sessions_exist(session_list, pi_project):
    start_time = time.time()
    while session_list:
        time.sleep(5)
        for session in session_list:
            if pi_project.sessions.find_first(f'copy_of={session.id}'):
                session_list.remove(session)
        if time.time() - start_time > WAIT_TIMEOUT:
            log.error("Wait timeout for move to complete")
            sys.exit(-1)

def main():
    file_ = gtk_context.get_input("input-file")
    breakpoint()
    
    acquisition = client.get_acquisition(file_["hierarchy"]["id"])
    project = client.get(acquisition.parents.project)
    site = project.group

    hdr_fields = get_hdr_fields(file_, site)
    if hdr_fields:
        pi_id = hdr_fields["pi_id"].casefold()
        pi_project_path = os.path.join(site, pi_id)
        if not pi_id.isalnum():
            print(f"pi_id: {pi_id} contains non-alphanumeric characters. Copying to 'other'.")
            pi_id = 'other'
    else:
        pi_id = 'other'
        
    pi_project_path = os.path.join(site, pi_id)
    pi_project = client.lookup(pi_project_path)
    
    # Smart copy to pi_project
    tmp_project_label = random.choices(string.ascii_lowercase + string.digits, k=10) # Generate random tmp project label
    tmp_project_id = smart_copy(site_project, 'tmp', to_copy_tag, tmp_project_label, True)["project_id"]
    tmp_project = client.get_project(tmp_project_id)
    check_smartcopy_loop(tmp_project)
    mv_to_project(tmp_project, pi_project)
    check_copied_sessions_exist(session_list, pi_project)
    delete_project(os.path.join('tmp', tmp_project_label))
    
    # Rename subject
    

if __name__ == "__main__":
    with flywheel_gear_toolkit.GearToolkitContext(fail_on_validation=False) as gtk_context:
        config = gtk_context.config
        client = gtk_context.client
        try:
            gtk_context.init_logging()
            status = main()
        except Exception as exc:
            log.exception(exc)
            status = 1

    sys.exit(status)
