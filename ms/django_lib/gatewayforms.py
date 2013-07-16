from django_lib import override_forms
from django import forms
from django_lib.override_forms import ReadOnlyWidget

class ModifyGatewayConfig(override_forms.MyForm):
  
  json_config = forms.FileField(required=False,
                                label="Gateway Configuration"
                                )


class ModifyGatewayLocation(override_forms.MyForm):

  host = forms.CharField(label="New Gateway host",
                           max_length = 499)

  port = forms.IntegerField(label="New Port number",
                            max_value=65535)


class GatewayRemoveVolume(override_forms.MyForm):

  volume_name = forms.CharField(label="Volume name",
                           widget=ReadOnlyWidget(),
                           required=False,
                           max_length=499)

  remove = forms.BooleanField(label="Remove",
                              required=False)

class GatewayAddVolume(override_forms.MyForm):

  volume_name = forms.CharField(label="Volume name",
                           max_length=499)

class CreateGateway(override_forms.MyForm):

	g_name = forms.CharField(label="Gateway name",
							initial="My Gateway",
							max_length=499,
							help_text="Your gateway's name cannot be changed later.")

	g_password = forms.CharField(label="Gateway password",
                              	max_length=499,
                               	widget=forms.PasswordInput)

	host = forms.CharField(label="Host name",
							max_length=499,)

	port = forms.IntegerField(label="Port number",
								max_value=65535)

class DeleteGateway(override_forms.MyForm):
    
    confirm_delete = forms.BooleanField(required=True,
                                        label="Yes, I understand that this action is permament and my gateway will be gone.")

    g_password = forms.CharField(label="Gateway password",
                               max_length=20,
                               widget=forms.PasswordInput,
                               help_text="You must also own this gateway to delete it.")

class CreateUG(CreateGateway):

    volume_name = forms.CharField(label="Volume name",
                           max_length=499)

    read_write = forms.BooleanField(required=False,
                                    label="UG can write to other gateways.")

# JSON config et al to come for these gateways.
class CreateAG(CreateGateway):
    
    json_config = forms.FileField(required=False,
                                  label="Gateway Configuration",
                                  help_text="If no file is specified, blank config will be used.")

    json_config_text = forms.CharField(required=False,
                                        max_length=5000,
                                        widget=forms.Textarea,
                                        label="Gateway Configuration (alternate)",
                                        help_text="This can also be used to manually config the gateway with text in JSON format. The upload file will take priority however.")

class CreateRG(CreateGateway):

    json_config = forms.FileField(required=False,
                                  label="Gateway Configuration",
                                  help_text="If no file is specified, blank config will be used.")

    json_config_text = forms.CharField(required=False,
                                        max_length=5000,
                                        widget=forms.Textarea,
                                        label="Gateway Configuration (alternate)",
                                        help_text="This can also be used to manually config the gateway with text in JSON format. The upload file will take priority however.")

    private = forms.BooleanField(required=False,
                                  label="Replica Gateway is private. It can only be attached to volumes owned by you.")