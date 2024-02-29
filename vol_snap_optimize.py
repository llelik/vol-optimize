################################################################
# NetApp ONTAP volume restore to a found snapshot 
# Author:  Alexey Mikhaylov
# Contact: alexey.mikhaylov@netapp.com
# release: 0.9
# (c)NetApp Professional Services Germany
#
# Summary: 1. The script restores a volume to a next after found 
#          snapshot to optimize volume space consumption
#          2. Sets volume guarantee to thin volume if set otherwise
#
################################################################


from netapp_ontap import config, HostConnection, NetAppRestError
from netapp_ontap.resources import Volume, Snapshot
import re, sys
from datetime import datetime
import argparse
from getpass import getpass
import logging

SNAPPREFIX = '^(NONE|LH|FREEZE)'

def pretty_dict(d, indent=0):
   for key, value in d.items():
      print('\t' * indent + str(key))
      if isinstance(value, dict):
         pretty_dict(value, indent+1)
      else:
         print('\t' * (indent+1) + str(value))

def confirm_restore(volume_name, snap_uuid: str):
    answer = ""
    while answer.lower() not in ["yes", "no"]:
      answer = input(f'Please confirm restoring volume {volume_name} to snapshot UUID {snap_uuid} [Yes,no]')
    if answer.lower() in ["yes"]:
      return True
    else:
      return False

'''
def set_volume_guarantee(vol_name, vol_uuid, cluster, guarantee: str):
  vol_data = {
        'uuid': vol_uuid,
        'guarantee': {'type': guarantee}
         }
  vol = Volume()
  try:
    config.CONNECTION = HostConnection(cluster, args.username, args.password, verify=False)
    with config.CONNECTION:
      return vol.patch(**vol_data) 
  except NetAppRestError as err:
      log.error(f"Setting volume guarantee to {guarantee} was not successful: {err}")
      return None
'''

def get_volume_type(vol_name, vol_uuid, cluster: str):
  try:
    with HostConnection(cluster, args.username, args.password, verify=False):
      vol = Volume(uuid=vol_uuid)
      vol.get(fields="type")
      return vol.type
  except NetAppRestError as err:
      log.error(f'Error reading type for volume {vol_name}: {err}')
      return None

def volume_restore_by_uuid(vol_name, vol_uuid, snap_insta_uuid, snap_name, vserver, cluster: str, dryrun: bool):
  """Restore Volume to a given UUID """
  vol_data = {
        'uuid': vol_uuid,
        'restore_to.snapshot.uuid': snap_insta_uuid,
         'validate_only': dryrun
         }
  vol = Volume()

  if not dryrun:
    log.info(f'''\n\n+ Restoring volume {vol_name} 
                on vserver  {vserver} 
                on cluster  {cluster} 
                to snapshot {snap_name} (next after the youngest)''')
  else:
    log.info(f'''\n\n *** DRY-RUN: Restoring volume {vol_name} 
                          on vserver {vserver} 
                          on cluster {cluster} 
                          to snapshot {snap_name} (next after the youngest) - only data validation execution''')
  try:
    with HostConnection(cluster, args.username, args.password, verify=False):
      return vol.patch(**vol_data) 
  except NetAppRestError as err:
      log.error(f'Volume restore was not successful: {err}')
      return None

def get_volume_uuid(vserver_name, volume_name, cluster: str):
    """List Volumes in a SVM """
    try:
      log.info(f'''+ Looking up volume {volume_name} on vserver {vserver_name} on cluster {cluster} ''')
      with HostConnection(cluster, args.username, args.password, verify=False):
        for vol in Volume.get_collection(**{"svm.name": vserver_name, "name": volume_name}):
            vol.get()
            return vol.uuid
    except NetAppRestError as err:
        log.error(f'Volume not found: {err.message}')
        return None
    

def find_last_snap(prefix, volume_uuid, cluster: str):
    """Find suitable last snapshot on a Volume """
    snaps_list = {}
    regex = re.compile(prefix)
    idx = 0
    prefix_snap = False
    try:
      config.CONNECTION = HostConnection(cluster, args.username, args.password, verify=False)
      with config.CONNECTION:
        log.info(f'Searching relevant snapshots on {config.CONNECTION.origin}')
        snapshots = Snapshot()
        for snap in snapshots.get_collection(volume_uuid, fields="create_time,version_uuid,name,volume,svm", order_by="create_time"):
          if regex.match(snap.name):
            snaps_list = {}
            snaps_list[0] = {"version_uuid": snap.version_uuid, "uuid": snap.uuid, "name": snap.name, "create_time": datetime.timestamp(snap.create_time), "ct_human": snap.create_time }
            idx = 0
            if args.verbose:
              print(f'Found relevant snapshot: {snaps_list[0]["name"]}')
            prefix_snap = True
          else:
            idx += 1
            snaps_list[idx] =  {"version_uuid": snap.version_uuid, "uuid": snap.uuid,  "name": snap.name, "create_time": datetime.timestamp(snap.create_time), "ct_human": snap.create_time  }
    except NetAppRestError as err:
        log.error(f'Snapshot not found: {err}')
    return snaps_list, prefix_snap

def find_snapshot_by_uuid(volume_uuid, snap_uuid, cluster: str):
    """Validate snapshot on volume """
    snapshot_found = False
    try:
      with HostConnection(cluster, args.username, args.password, verify=False):
        snap = Snapshot(volume_uuid, uuid = snap_uuid)
        snap.get()
        return snap.version_uuid
    except NetAppRestError as err:
        log.error(f'Snapshot not found: {err}')
        return None

def list_all_snapshots(volume_name, volume_uuid, cluster: str):
    """List all snapshots """
    try:
      config.CONNECTION = HostConnection(cluster, args.username, args.password, verify=False)
      with config.CONNECTION:
        snapshots = Snapshot()
        log.info(f'All snapshots on cluster {config.CONNECTION.origin} in volume {volume_name}:')
        for snap in snapshots.get_collection(volume_uuid, fields="create_time,version_uuid,name,volume,svm", order_by="create_time"):
          print(f'{snap.version_uuid},  {snap.name},  {snap.create_time}')
    except NetAppRestError as err:
        log.error(f'Snapshot not found: {err}')
        return None

def get_prefix_snapshots_list(prefix, volume_name, volume_uuid, cluster: str):
    """List snapshots with a given prefix """
    regex = re.compile(prefix)
    snaps_list = {}
    try:
      with HostConnection(cluster, args.username, args.password, verify=False):
        snapshots = Snapshot()
        for snap in snapshots.get_collection(volume_uuid, fields="create_time,version_uuid,name,volume,svm", order_by="create_time"):
          if regex.match(snap.name):
            snaps_list[snap.version_uuid] = snap.name
    except NetAppRestError as err:
        log.error(f'Snapshots not found: {err}')
    return snaps_list

def print_summary_pre():
  summary: str = ""
  summary += f'''
  Target: 
     cluster:        {args.cluster}
     volume:         {args.volume}
     rel. snapshot:  {last_snapshot_list[0]["name"]}  *** {last_snapshot_list[0]["version_uuid"]} *** {last_snapshot_list[0]["ct_human"]}
  '''
  if not args.skip_src_validation:
    snap_found =  f'Yes, UUID = {is_snapshot_on_source}' if (is_snapshot_on_source != None) and (is_snapshot_on_source == last_snapshot_list[0]["version_uuid"]) else  f'*** NOT FOUND ***'
    summary += f'''
  Source:
    cluster:         {args.source_cluster}
    volume:          {args.source_volume}
    rel. snap found: {snap_found}
    '''
  summary += f'''
  Suitable restore snapshot: 
     Name:           {last_snapshot_list[1]["name"]}
     Create_time:    {last_snapshot_list[1]["ct_human"]}
     UUID:           {last_snapshot_list[1]["version_uuid"]}
  '''

  summary += f'''
  All snapshots after relevant one will be deleted:
  '''
  for k, v in last_snapshot_list.items():
    if k > 0:
      summary += f'''
      id: {k} Name: {v["name"]} Create_time: {v["ct_human"]} 
  '''
  return summary

def parse_args() -> argparse.Namespace:
    """Parse the command line arguments from the user"""

    parser = argparse.ArgumentParser(
        description="This script will restore the volume to the recent [+ 1] affected snapshot."
    )
    parser.add_argument(
        "-sc", "--src_cluster", "--source_cluster", dest="source_cluster", required=False, help="Source cluster"
    )
    parser.add_argument(
        "-c", "--cluster", "--target_cluster", required=True, help="Target cluster"
    )
    parser.add_argument(
        "-sv", "--src_vol", "--source_volume", dest="source_volume", required=False, help="Source Volume to validate against"
    )
    parser.add_argument(
        "-s_svm", "--src_vserver", "--source_vserver", dest="source_vserver", required=False, help="Source vserver with volume to validate against"
    )
    parser.add_argument(
        "--volume", "-vol", dest="volume", required=True, help="Volume on which restoration is executed"
    )
    parser.add_argument(
        "-svm", "--vserver", "--target_vserver", required=True, help="SVM on which volume must be restored"
    )
    parser.add_argument(
        "-debug", "--debug", dest="debug", action='store_true', default=False, required=False, help="Debug output enabled"
    )
    parser.add_argument(
        "--verbose", dest="vorbose", action='store_true', default=False, required=False, help="More verbose"
    )
    parser.add_argument(
        "-dryrun", "--dryrun", dest="dryrun", action='store_true', default=False, required=False, help="Dry-run, no restore, only finding right snapshots and validating details"
    )
    parser.add_argument(
        "--skip_src_validation", "--skip_source_validation", "-skip_src_validation", dest="skip_src_validation", required=False, action='store_true', default=False, help="Skip source cluster snapshot validation"
    )
    parser.add_argument(
        "--guarantee", dest="guarantee", required=False, help="Dry-run, no restore, only finding right snapshots and validating details"
    )
    parser.add_argument("-u", "--api_user", "--username", dest="username", default="admin", help="API Username")
    parser.add_argument("-p", "--api_pass", "--password", dest="password", help="API Password")
    parsed_args = parser.parse_args()

    # collect the password without echo if not already provided
    if not parsed_args.password:
        parsed_args.password = getpass()

    return parsed_args


if __name__ == "__main__":
  
  args = parse_args()
  if args.debug:
    logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] [%(levelname)5s] [%(module)s:%(lineno)s] %(message)s")
  else: 
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)5s] %(message)s")
  log = logging.getLogger('snapshots')
  
  volume_uuid = get_volume_uuid(args.vserver, args.volume, args.cluster)
  if volume_uuid != None:
    log.info(f'''++ Found volume {args.volume} UUID = {volume_uuid} 
                      on cluster {args.cluster}''')

  target_prefix_snaps = get_prefix_snapshots_list(SNAPPREFIX, args.volume, volume_uuid, args.cluster)
  
  if volume_uuid == None:
    log.error('No target volume found on SVM. Exiting.')
    quit()

  if get_volume_type(args.volume, volume_uuid, args.cluster).lower() != "rw":
    log.error(f'''\n-- Volume {args.volume} type is not RW. Restore is not possible.
      To proceed volume type must be RW (Snapmirror destination?)''')
    quit()
  
  log.setLevel('DEBUG')
  # print all snapshots on target volume on target cluster
  list_all_snapshots(args.volume, volume_uuid, args.cluster)
  log.setLevel('INFO')

  # identify the last snapshot to restore to
  last_snapshot_list, snapshot_found = find_last_snap(SNAPPREFIX, volume_uuid, args.cluster)

  # if snapshot for restore found on target
  if len(last_snapshot_list) > 1 and snapshot_found:
    log.info(f'''The youngest relevant snapshot is found: 
                  UUID: {last_snapshot_list[0]["version_uuid"]}
                  name: {last_snapshot_list[0]["name"]}
                  Create time: {last_snapshot_list[0]["ct_human"]}''')
  elif len(last_snapshot_list) == 1:
    log.info(f'''Relevant snapshot is the last snapshot in the volume:
              {last_snapshot_list[0]["name"]}
              -- No snapshots to optimize the volume!
              ''')
    quit()
  else:
    log.error(f'\nRelevant snapshot for restoration is not found. Exiting.')
    quit()

  if args.skip_src_validation:
    log.warning("!! Skipping Source volume snapshots validation as requested...")
    
  else: # if we don't skip source validation
    source_volume_uuid = get_volume_uuid(args.source_vserver, args.source_volume, args.source_cluster)
    if source_volume_uuid != None:
      log.info(f'''++ Found volume {args.source_volume} UUID = {source_volume_uuid} 
                      on cluster {args.source_cluster}''')
    else:
      quit()
    source_prefix_snaps = get_prefix_snapshots_list(SNAPPREFIX, args.source_volume, source_volume_uuid, args.source_cluster)
    snap_src_tgt_diff = set(target_prefix_snaps)^set(source_prefix_snaps)    
    if len(snap_src_tgt_diff) > 0:
      log.error(f'''ATTENTION:
        Affected snapshots on source and destination are not the same. 
        Missing either on source or dest snapshots: 
        {snap_src_tgt_diff}
        Exiting.''')
      #quit()

    log.info(f'Validating snapshot on source... {args.source_cluster}')
    is_snapshot_on_source = find_snapshot_by_uuid(source_volume_uuid, last_snapshot_list[0]["version_uuid"], args.source_cluster) 

    if (is_snapshot_on_source != None) and (is_snapshot_on_source == last_snapshot_list[0]["version_uuid"]):
      log.info(f'''
        Relevant young snapshot exists on source cluster {args.source_cluster}
        Volume can be restored to the next avaiable snapshot: {last_snapshot_list[1]["name"]}
        ''')
    else: 
      log.error(f'Relevant snapshot {last_snapshot_list[0]["name"]} cannot be validated on source cluster {args.source_cluster}. Exiting.')
      quit()

print("\nPre-execution summary:\n", print_summary_pre())

log.setLevel('DEBUG')
# print all snapshots on target volume on target cluster
log.debug(f''' *** DEBUG: Listing all snapshots 
           Target cluster: {args.cluster} 
           Volume:         {args.volume}''')
list_all_snapshots(args.volume, volume_uuid, args.cluster)
log.setLevel('INFO')

if args.source_volume and args.source_cluster and args.source_vserver:
  # print all snapshots on source volume on source cluster
  log.debug(f''' *** DEBUG: Listing all snapshots
             Source cluster: {args.source_cluster} 
             Volume:         {args.source_volume}''')
  list_all_snapshots(args.source_volume, source_volume_uuid, args.source_cluster)


# if execution is not dry-run
if not args.dryrun:
  # we need console confirmation
  if confirm_restore(last_snapshot_list[1]["name"], last_snapshot_list[1]["version_uuid"]):
    log.info("Shit gets real...")
    # executing restore
    vol_restore = volume_restore_by_uuid(args.volume, volume_uuid, last_snapshot_list[1]["uuid"], last_snapshot_list[1]["name"], args.vserver, args.cluster, False)
    if vol_restore:
      log.info(f'Volume was restored successfully. \n New snapshot list:')
      list_all_snapshots(args.volume, volume_uuid, args.cluster)
      
      ''' not implemented
      log.info(f"Setting volume guarantee to none... ")
      if args.guarantee:
        set_volume_guarantee(args.volume, volume_uuid, args.cluster, args.guarantee)
      '''
      
  # restore is not confirmed
  else: 
    log.info(f'Volume restore is cancelled by operator. Exiting. ')
    quit()
# dry-run exec
else:
  log.info("Executing dry-run...")
  vol_restore = volume_restore_by_uuid(args.volume, volume_uuid, last_snapshot_list[1]["uuid"], last_snapshot_list[1]["name"], args.vserver, args.cluster, True)
  if vol_restore:
    log.info(f'++ Dry-run did not detect any issues')
  else: 
    log.error(f'-- Dry-run has failed')

