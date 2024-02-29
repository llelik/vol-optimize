################################################################
# Setting NetApp ONTAP volume space guarantee to a requested value 
# Author:  Alexey Mikhaylov
# Contact: alexey.mikhaylov@netapp.com
# release: 0.9
# (c)NetApp Professional Services Germany
#
# Summary: Sets volume guarantee to thin volume if set otherwise
# Possible values: [volume, none]
################################################################

from netapp_ontap import config, HostConnection, NetAppRestError
from netapp_ontap.resources import Volume, Snapshot
import sys
import argparse
from getpass import getpass
import logging

def set_volume_guarantee(vol_name, vol_uuid, cluster, guarantee: str):
  try:
    with HostConnection(cluster, args.username, args.password, verify=False):
    	vol = Volume(uuid=vol_uuid)
    	vol.guarantee = {'type': guarantee}
    	vol.patch()
    	return vol 
  except NetAppRestError as err:
      log.error(f"Setting volume guarantee to {guarantee} was not successful: {err}")
      return None

def get_volume_uuid(vserver_name, volume_name, cluster: str):
    """List Volume uuid and guarantee in an SVM """
    try:
      with HostConnection(cluster, args.username, args.password, verify=False):
        for vol in Volume.get_collection(**{"svm.name": vserver_name, "name": volume_name}):
            vol.get(fields="uuid,guarantee")
            return vol.uuid, vol.guarantee.type
    except NetAppRestError as err:
        log.error(f'Volume not found: {err}')
        return None

def get_volume_type(vol_name, vol_uuid, cluster: str):

	""" Retrieving volume type """
	try:
		with HostConnection(cluster, args.username, args.password, verify=False):
			vol = Volume(uuid=vol_uuid)
			vol.get(fields="type")
			return vol.type
	except NetAppRestError as err:
		log.error(f'Error reading type for volume {vol_name}: {err}')
		return None

def parse_args() -> argparse.Namespace:
    """Parse the command line arguments from the user"""

    parser = argparse.ArgumentParser(
        description="This script will restore the volume to the recent [+ 1] affected snapshot."
    )
    parser.add_argument(
        "-c", "--cluster", "--target_cluster", required=True, help="Target cluster"
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
        "--guarantee", dest="guarantee", required=True, help="Dry-run, no restore, only finding right snapshots and validating details"
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
		log = logging.getLogger('volGuarantee')

	log.info(f"Looking up for volume {args.volume} on cluster {args.cluster} and checking it's capabilities...")
	volume_uuid, volume_guarantee = get_volume_uuid(args.vserver, args.volume, args.cluster)
	
	

    # if volume was found and UUID,gurantee were retrieved
	if volume_uuid != None:
		log.info(f"++ Found volume with vol_UUID = {volume_uuid} with guarantee set to {volume_guarantee}")

        # we exit if volume type is not RW, no changes can be made to this vol type. Volume must be brought to RW first
		if get_volume_type(args.volume, volume_uuid, args.cluster).lower() != "rw":
			log.error(f'''\n-- Volume {args.volume} type is not RW. Restore is not possible.
			            To proceed volume type must be RW (Snapmirror destination?)''')
			quit()

		if args.guarantee and args.guarantee.lower() != volume_guarantee.lower():
			log.info(f"Setting volume guarantee to {args.guarantee}... ")
			set_guarantee_resp = set_volume_guarantee(args.volume, volume_uuid, args.cluster, args.guarantee)

			# Re-reading volume guarantee after changes
			volume_uuid, volume_guarantee = get_volume_uuid(args.vserver, args.volume, args.cluster)
			log.info(f"Now volume {args.volume} guarantee: {volume_guarantee}")
			
			if set_guarantee_resp == None:
				log.error(f"-- Cannot set volume guarantee to {args.guarantee} due to previous errors")
		else: 
			log.info(f"Volume guarantee is already {volume_guarantee}. No action needed.")
	# volume not found, error is reported in function
	else:
		quit()
  
  
