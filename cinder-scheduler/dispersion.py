"""
Cinder Scheduler Example
"""

from cinder.openstack.common import log as logging

from cinder import db
from cinder import exception
from cinder.scheduler import filter_scheduler
from cinder.scheduler import driver
from cinder import utils

LOG = logging.getLogger(__name__)


class Dispersion(filter_scheduler.FilterScheduler):

    def schedule_create_volume(self, context, request_spec, filter_properties):
        """Use volume type in extra_specs to identify tenant for tenant isolation"""

        volume_id = request_spec.get('volume_id')
        snapshot_id = request_spec.get('snapshot_id')
        image_id = request_spec.get('image_id')
        volume_properties = request_spec.get('volume_properties')
        availability_zone = volume_properties.get('availability_zone')

        context_dict = context.to_dict()
        tenant_name = context_dict['project_name']

        # check if request has volume type and volume type matching tenant
        # if no volume type in request, search db for tenant's bind volume type
        # if no bind volume type, add default volume type to create volume
        volume_type = request_spec.get('volume_type')
        if volume_type:
            specs = volume_type.get('extra_specs')
            if 'tenant_name' in specs:
                if specs['tenant_name'] != tenant_name:
                    msg = _("Tenant cannot use volume type %s." % 
                              volume_type['name'])
                    raise exception.InvalidVolumeType(reason=msg)
        else:
            #check db if user's tenant has been bond to a volume type
            bindType = False
            volume_types = db.volume_type_get_all(context)
            for key in volume_types:
                specs = volume_types[key].get('extra_specs')
                if 'tenant_name' in specs:
                    if specs['tenant_name'] == tenant_name:
                        bindType = True
                        request_spec['volume_type'] = volume_types[key]
                        break
            if not bindType:
                request_spec['volume_type'] = db.volume_type_get_by_name(
                        context, 'DEFAULT')

        LOG.debug(str(request_spec))
        
        host = 'MyHost'
        updated_volume = driver.volume_update_db(context, volume_id, host)
        self.volume_rpcapi.create_volume(context, updated_volume, host,
                                         request_spec, filter_properties,
                                         snapshot_id=snapshot_id,
                                         image_id=image_id)
        return None
