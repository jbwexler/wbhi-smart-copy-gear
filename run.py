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

from wbhiutils import parse_dicom_hdr

log = logging.getLogger(__name__)

DATE_FORMAT_FW = "%Y%m%d"
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
    hdr_fields["pi_id"], hdr_fields["sub-id"] = parse_dicom_hdr.parse_pi_sub(dcm_hdr, site)
    
    return hdr_fields

def smart_copy(
    src_project,
    group_id: str = None,
    acquisition: str = None,
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
    
    while True:
        dst_project_path = os.path.join(group_id, dst_project_label)
        try:
            client.lookup(dst_project_path)
            if delete_existing_project:
                delete_project(dst_project_path)
            else:
                dst_project_label = dst_project_label + '_1'
        except flywheel.rest.ApiException:
            break

    data = {
        "group_id": group_id,
        "project_label": dst_project_label,
        "filter": {
            "exclude_analysis": False,
            "exclude_notes": False,
            "exclude_tags": False,
            "include_rules": [],
            "exclude_rules": [],
        },
    }
    session = client.get_session(acquisition.session)
    subject = client.get_subject(acquisition.parents.subject)
    
    data["filter"]["include_rules"].append(f"acquisition.label={acquisition.label}")
    data["filter"]["include_rules"].append(f"session.label={session.label}")
    data["filter"]["include_rules"].append(f"subject.label={subject.label}")
    
    print(f'Smart copying "{src_project.group}/{src_project.label}" to "{group_id}/{dst_project_label}')

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
        for acquisition in session.acquisitions.iter():
            try:
                acquisition.update(project=dst_project.id)
            except flywheel.ApiException as exc:
                if exc.status == 422:
                    log.error(
                        f"{session.subject.label}/{session.label}/{acquisition.label} already exists in {dst_project.label} - Skipping"
                    )
                else:
                    log.exception(
                        f"Error moving {session.subject.label}/{session.label}/{acquisition.label} from {src_project.label} to {dst_project.label}"
                    )
        
def check_copied_acq_exists(acquisition, pi_project):
    start_time = time.time()
    while True:
        time.sleep(3)
        for session in pi_project.sessions.iter():
            if session.acquisitions.find_first(f'copy_of={acquisition.id}'):
                return session
            elif time.time() - start_time > WAIT_TIMEOUT:
                log.error("Wait timeout for move to complete")
                sys.exit(-1)
                
def delete_project(project_path):
    try: 
        project = client.lookup(project_path)
        client.delete_project(project.id)
        print(f"Successfully deleted project {project_path}")
    except flywheel.rest.ApiException:
        print(f"Project {project_path} does not exist")
        
def get_first_dicom(session):
    acq_list = session.acquisitions()
    acq_sorted = sorted(acq_list, key=lambda d: d.timestamp)
    if not acq_sorted:
        return None
    file_list = acq_sorted[0].files
    dicom = [f for f in file_list if f.type == "dicom"][0]
    return dicom

def main():
    file_id = gtk_context.get_input("input-file")["object"]["file_id"]
    file_ = client.get_file(file_id)
    acquisition = client.get_acquisition(file_.parents.acquisition)
    project = client.get_project(file_.parents.project)
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
    
    # Smart copy to pi_project
    pi_project_path = os.path.join(site, pi_id)
    try:
        pi_project = client.lookup(pi_project_path)
        tmp_project_label = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)) # Generate random tmp project label
        tmp_project_id = smart_copy(project, 'tmp', acquisition, tmp_project_label, True)["project_id"]
        tmp_project = client.get_project(tmp_project_id)
        check_smartcopy_loop(tmp_project)
        mv_to_project(tmp_project, pi_project)
        delete_project(os.path.join('tmp', tmp_project_label))
    except flywheel.rest.ApiException:     
        new_project_id = smart_copy(project, site, acquisition, pi_id, True)["project_id"]
        new_project = client.get_project(new_project_id)
        check_smartcopy_loop(new_project)
    
    # Move original to renamed subject
    session = check_copied_acq_exists(acquisition, pi_project)
    dicom = get_first_dicom(session)
    hdr_fields_first = get_hdr_fields(dicom, site)
    new_sub_name_fields = (
        hdr_fields_first['pi_id'], 
        hdr_fields_first['sub_id'],
        hdr_fields_first['date'].strftime(DATE_FORMAT_FW),
        "AM" if hdr_fields_first["before_noon"] else "PM"
    )
    new_sub_name = "%s_%s_%s_%s" % (new_sub_name_fields)
    # Restrict subject label to 64 chars
    if len(new_sub_label) > 64:
        new_sub_label = new_sub_label[:64]
    print(new_sub_label)
    #acquisition.update(subject=dst_project.id)
    
    # Tag acquisition
    tag = f"smart-copy-{pi_id}"
    if tag not in acquisition.tags:
        acquisition.add_tag(tag)

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
