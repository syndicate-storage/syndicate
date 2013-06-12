import sys
sys.path.append("/usr/share/SMDS")

from SMDS.mdapi import MDAPI
from SMDS.faults import *
from SMDS.mdserver import *
from SMDS.user import *
from SMDS.content import *
from SMDS.web2py.extras import *
from SMDS.Methods import *

import traceback

SMDS_api = MDAPI()

ENOADDUSER = 1
EINVALIDUSER = 2
EINVALIDVOLUME = 3
ENODELUSER = 4
ENOMDCONTACT = 5
ECDNAPI = 6

error_msgs = {
    ENOADDUSER:      "Could not add user",
    EINVALIDUSER:    "Invalid user",
    EINVALIDVOLUME:  "Invalid volume",
    ENODELUSER:      "Could not remove user",
    ENOMDCONTACT:    "Could not contact volume metadata server",
    ECDNAPI:         "Could not contact CDN"
}

# -*- coding: utf-8 -*-
### required - do no delete
def user(): 
   return dict(form=auth())

def download(): return response.download(request,db)
def call():
    session.forget()
    return service()
### end requires

@auth.requires_login()
def index():
    # Depending on whether or not we're an admin or not,
    # show links to the user's volumes, account, and optionally
    # admin tools.
    global SMDS_api
    global error_msgs
    
    auth = current.session.auth
    
    user_tools = TABLE()
    
    if auth != None and auth.user != None and 'user' in auth.user.roles:
        user_tools.append(TR(TH('User Tools')))
        user_tools.append(TR(TD(A("Account", _href=URL(r=request, f="my_account"), _name="Account"))))
        user_tools.append(TR(TD(A("Volumes", _href=URL(r=request, f="volumes"), _name="Volumes"))))
        user_tools.append(TR(TD(A("Content Servers", _href=URL(r=request, f="contents"), _name="Contents"))))
    
    admin_tools = TABLE()
    
    if auth != None and auth.user != None and 'admin' in auth.user.roles:
       admin_tools.append(TR(TH('Admin Tools')))
       admin_tools.append(TR(TD(A("Users", _href=URL(r=request, f="users"), _name="Users"))))
    
    return dict(user_tools=user_tools, admin_tools=admin_tools)
    
    

# quickie functions to select meaningful names for field items
def edit_button_name( obj_id ):
    return "edit_" + str(obj_id)
    
def delete_button_name( obj_id ):
    return "delete_" + str(obj_id)
    
def mdserver_form_name( name ):
    return "mdserver_" + str(name)

def content_form_name( name ):
    return "content_" + str(name)
    
def user_form_name( name ):
    return "user_" + str(name)
    
def is_mdserver_var( name ):
    return name.startswith("mdserver_")
    
def is_user_var( name ):
    return name.startswith("user_")

def is_content_var( name ):
    return name.startswith("content_")
    
def mdserver_key( form_name ):
    return form_name[len("mdserver_"):]

def user_key( form_name ):
    return form_name[len("user_"):]
    
def content_key( form_name ):
    return form_name[len("content_"):]
    
    
@auth.requires_login()
def users():
    """
    Control panel for enabling/disabling users
    """
    return dict()
    

def build_content_row( api, content ):
   """
   Build up a form row that represents a content server
   """
   row = TR( TD( content['host_url'] ), TD( INPUT( _type="submit", _value="Delete", _name=delete_button_name( content['content_id'] ) ) ) )
   
   return row
   


def build_server_row( api, server ):
    """
    Build up a static form row that represents a server.
    """
    
    authstr = ""
    if server['auth_read'] and not server['auth_write']:
        authstr = "read"
    elif server['auth_read'] and server['auth_write']:
        authstr = "read/write"
    else:
        authstr = "write"
    
    # look up the users
    md_usernames = []
    if len(server['user_ids']) > 0:
        md_users = Users( api, server['user_ids'] )
        md_usernames = [u['username'] for u in md_users]
    
    row = TR( TD(INPUT(_type="submit", _value="Edit", _name=edit_button_name(server['server_id'])), 
              TD(INPUT(_type="submit", _value="Delete", _name=delete_button_name(server['server_id']))), TD(server['name']), TD(server['host']), \
              TD(str(server['portnum'])), TD(server['status']), TD(authstr), TD(", ".join(md_usernames)) )) 
    
    return row
    

def build_server_row_edit( api, server ):
    """
    Build up an editable form row that represents a server.
    """
    row = TR()
    
    # submit button
    row.append( TD(INPUT(_type="submit", _value="Submit", _name="submit_" + str(server['server_id'])) ) )
    
    # cancel button
    row.append( TD(INPUT(_type="submit", _value="Cancel", _name="cancel")) )
    
    # name can't be changed
    row.append( TD(server['name']) )
    
    # hostname can't be chaned
    row.append( TD(server['host']) )
    
    # port number
    row.append( TD(INPUT(_type="text", _value=str(server['portnum']), _maxlength="5", \
                         _style="display:table-cell; width:100px", _name=mdserver_form_name("portnum"), requires=IS_INT_IN_RANGE(1025, 65534))) )
    
    # status
    status_opts = TABLE(_name="status")
    status_opts.append( TR(INPUT(_type="radio", _name=mdserver_form_name("status"), _value="running", value=server['status']), 'Running') )
    status_opts.append( TR(INPUT(_type="radio", _name=mdserver_form_name("status"), _value="stopped", value=server['status']), 'Stopped') )
    row.append( TD(status_opts) )
    
    # read/write authentication
    auth_opts = TABLE(_name="auth")
    read_val = ''
    write_val = ''
    
    if server['auth_read']: read_val = 'on'
    if server['auth_write']: write_val = 'on'
    
    auth_opts.append( TR(INPUT(_type="checkbox", _name=mdserver_form_name("auth_read"), value=read_val), 'Read')) 
    auth_opts.append( TR(INPUT(_type="checkbox", _name=mdserver_form_name("auth_write"), value=write_val), 'Write')) 
    row.append( TD(auth_opts) )
    
    # look up the users
    md_users = []
    if len(server['user_ids']) > 0:
        md_users = Users( api, server['user_ids'] )
    
    # users
    user_opts = TABLE(_name="users")
    for user in md_users:
        # only display enabled users
        if not user['enabled']:
            continue
            
        user_opts.append( TR(INPUT(_type="checkbox", _name=user_form_name(user['user_id']), value='on', _align="right"), user['username']) )
        
    user_opts.append(TR( INPUT(_type="submit", _name="adduser", _value="Add User"), \
                         INPUT(_type="text", _maxlength="128", _style="display:table-cell; width:100px", _name="new_user", _value="", requires=SMDS_validators.IS_SMDS_USER())) )
    
    row.append( TD(user_opts) )
    return row
    
    
def build_volumes_table(api, user, edit_id):
    """
    Build up a table of the user's Syndicate volumes.
    edit stores the ID of the server to be editing, if given
    """
    mdservers = MDServers( api, user['my_mdserver_ids'] )
    volume_table = TABLE()
    volume_table.append( TR( TH(' '), TH(' '), TH('Name'), TH('Host'), \
                             TH('Port'), TH('Status'), TH('Authentication'), TH('Users') ))
    
    try:
        edit = int(edit)
    except:
        edit = None
    
    mdservers.sort( key=(lambda m: m['name']) )
    
    for server in mdservers:
        if edit_id != None and server['server_id'] == int(edit_id):
            volume_table.append( build_server_row_edit( api, server ) )
        else:
            volume_table.append( build_server_row( api, server ) )
    
    return volume_table
    


def build_content_table( api, user ):
   """
   Build up a table of the user's content servers
   """
   contents = Contents( api, user['content_ids'] )
   content_table = TABLE()
   
   content_table.append( TR( TH('URL'), TH(' ') ) )
   
   for content in contents:
      content_table.append( build_content_row( api, content ) )
   
   return content_table


def is_edit( request, v, edit="Edit"):
    """
    Was an \"edit server\" button pressed?
    """
    return v.startswith("edit_") and request.vars.get(v) == edit


def is_submit( request, v, value, name ):
    """
    Was a \"submit\" button pressed?
    """
    return v == value and request.vars.get(v) == name


def edit_server_redirect( request, v, new_vars ):
    # edit button was pushed.
    # find out which server that corresponds to,
    # and redirect to the volumes page with that
    # server being edited.
    server_id = -1
    try:
        server_id = int(v.split("_")[1])
    except:
        pass
    
    if server_id > 0:
        new_vars.update( dict(server_id=server_id) )
        redirect(URL(r=request, f='volumes', vars=new_vars))
        return True
    
    return False


def load_mdserver( api, server_id ):
    mdserver = None
    try:
        mdservers = MDServers( api, [server_id] )
        mdserver = mdservers[0]
    except:
        pass
        
    return mdserver
    

@auth.requires_login()
def reload_mdserver( api, user, mdserver, vars ):
    # stop the server, if we need to
    need_start = False
    smds_auth = {'AuthMethod':'password', 'Username':str(user['username'])}
        
    if mdserver['status'] == 'running':
        stopServer = StopMetadataServer.StopMetadataServer( api )
        stopServer.caller = user
        rc = -1
        try:
            rc = stopServer.call( smds_auth, mdserver['server_id'] )
        except MDMetadataServerError, e:
            # could not contact metadata server
            rc = -1
        
        if rc < 0:
            # cannot proceed
            return ENOMDCONTACT
        
        need_start = True
        
    update_fields = {
        'auth_read': False,
        'auth_write': False
    }
    delete_users = mdserver['user_ids']
    
    for (varname, varval) in vars.items():
        if is_mdserver_var( varname ):
            key = mdserver_key( varname )
            if key in MDServer.fields.keys():
                param_type = MDServer.fields[ key ].type
                val = param_type( varval )
                update_fields[key] = val
            
        elif is_user_var( varname ):
            key = user_key( varname )
            if key not in User.fields.keys():
                # this is a user ID.
                delete_users.remove( int(key) )
    
    
    # remove any users 
    worst_rc = 0
    if len(delete_users) > 0:
        rc = -1
        delUser = DeleteUserFromMetadataServer.DeleteUserFromMetadataServer( api )
        delUser.caller = user

        try:
            rc = delUser.call( smds_auth, delete_users, mdserver['server_id'] )
        except MDMetadataServerError, e:
            # could not apply changes to the server
            rc = -1
        
        if rc < 0:
            worst_rc = ENOMDCONTACT

    if worst_rc == 0:
        # removed all users we needed to; commit the update
        rc = -1
        updateServer = UpdateMetadataServer.UpdateMetadataServer( api )
        updateServer.caller = user
        
        try:
            rc = updateServer.call( smds_auth, mdserver['server_id'], update_fields )
        except MDMetadataServerError, e:
            # could not apply changes to the server
            rc = -1
        
        if rc < 0:
            worst_rc = ENOMDCONTACT
            
    if worst_rc == 0 and update_fields['status'] == 'running':
        # start the metadata server back up again 
        startServer = StartMetadataServer.StartMetadataServer( api )
        startServer.caller = user
        rc = -1
        try:
            rc = startServer.call( smds_auth, mdserver['server_id'] )
        except MDMetadataServerError, e:
            # could not contact metadata server
            rc = -1
        
        if rc == -1:
            # cannot proceed
            worst_rc = ENOMDCONTACT
        else:
            # rc is the (read url, write url) tuple
            worst_rc = rc
    
    return worst_rc        
     

@auth.requires_login()
def volumes():
    """
    Control panel to manage a user's Syndicate volumes
    """
    
    global SMDS_api
    global error_msgs
    
    auth = current.session.auth
    
    api = SMDS_api
    volume_form = FORM(_name="volume_form")
    vars = request.vars
    new_vars = {}
    
    if request.vars.get('server_id',None) != None:
        new_vars['server_id'] = request.vars.get('server_id')
        
    # do we have an error message?
    err = request.vars.get('error',None)
    try:
       err = int(err)
    except:
       pass

    if err and error_msgs.get(err) != None:
        volume_form.append( H3("ERROR: %s" % error_msgs.get(err), _style="color:#EF0000") )
    
    # do we have read/write handles?
    read_handle = request.vars.get('read', None)
    write_handle = request.vars.get('write', None)
    mdserver_name = request.vars.get('name', '')
    if read_handle or write_handle:
        rw_tbl = TABLE()
        if read_handle:
            rw_tbl.append( TR( TD( B(mdserver_name + " read handle:") ), TD( read_handle ) ) )
        if write_handle:
            rw_tbl.append( TR( TD( B(mdserver_name + " write handle:") ), TD( write_handle ) ) )
        
        volume_form.append( rw_tbl )
        volume_form.append( BR() )

        
    # build up a table of the user's syndicate volumes
    if len(auth.user['my_mdserver_ids']) == 0:
        volume_form.append( H3("No Volumes Defined") )
    else:
        volume_table = build_volumes_table( api, auth.user, request.vars.get('server_id',None) )
        volume_form.append( volume_table )
     
    volume_form.append( INPUT(_type="submit", _name="new volume", _value="New Volume...") )
    
    if volume_form.accepts( request.vars, session, formname="volume_form" ):

        for v in request.vars.keys():
            if is_edit( request, v, edit="Edit" ):
                if edit_server_redirect( request, v, new_vars ):
                    break
            
            elif is_submit(request, v, "new volume", "New Volume..."):
                # create a new volume
                redirect(URL(r=request, f='create_volume', vars={}))
                
            elif is_submit(request, v, "cancel", "Cancel"):
                # cancel button was pushed (i.e. from an edit)
                # re-build the table accordingly
                redirect(URL(r=request, f='volumes', vars={}))
            
            elif v.startswith("submit_") and request.vars.get(v) == "Submit":
                # the submit button was pushed (i.e. from an edit)
                # update the database and carry out any appropriate actions
                # find out which server that corresponds to
                server_id = -1
                try:
                    server_id = int(v.split("_")[1])
                except:
                    pass
                
                if server_id > 0:
                    mdserver = load_mdserver( api, server_id )
                    if not mdserver:
                        new_vars.update(dict(error=EINVALIDVOLUME))
                        redirect(URL(r=request, f='volumes', vars=new_vars))
                    
                    else:
                        rc = reload_mdserver( api, auth.user, mdserver, request.vars )
                        if isinstance(rc, tuple) or isinstance(rc, list):
                            # got back read/write handles
                            try:
                                read_handle = rc[0]
                                write_handle = rc[1]
                                new_vars['read'] = read_handle
                                new_vars['write'] = write_handle
                                new_vars['name'] = mdserver['name']
                            except:
                                pass
                            
                        elif rc != 0:
                            new_vars.update(dict(error=rc))
                        
                        del new_vars['server_id']        # no longer editing
                        redirect( URL(r=request, f='volumes', vars=new_vars) )
                        
                        
                pass
            
            
            elif v.startswith("delete_") and request.vars.get(v) == "Delete":
                # the delete button was pushed
                # update the database and carry out any appropriate actions
                # find out which server that corresponds to
                server_id = -1
                try:
                    server_id = int(v.split("_")[1])
                except:
                    pass
                
                if server_id > 0:
                    rc = remove_mdserver( api, auth.user, server_id )
                    if rc < 0:
                       new_vars.update(dict(error=ENOMDCONTACT))
                       
                    redirect( URL(r=request, f='volumes', vars=new_vars) )
                        
                        
                pass
            
            
            elif is_submit( request, v, "adduser", "Add User" ):
                # the Add User button was pushed (i.e. from an edit)
                # add the user to the metadata server
                mdserver_id = request.vars.get('server_id',None)
                
                if not mdserver_id:
                    new_vars.update(dict(error=EINVALIDVOLUME))
                    redirect(URL(r=request, f='volumes', vars=new_vars))
                else:
                    # look this user up
                    user_to_add = request.vars.get('new_user', None)
                    if not user_to_add:
                        user_to_add = ""
                    
                    new_user = None 
                    try:
                        new_user = Users( api, {'username': user_to_add})[0]
                    except:
                        new_vars.update(dict(error=EINVALIDUSER))
                        redirect(URL(r=request, f='volumes', vars=new_vars))
                    else:
                        rc = -1
                        addUser = AddUserToMetadataServer.AddUserToMetadataServer( api )
                        addUser.caller = auth.user
                        
                        try:
                            rc = addUser.call( {'AuthMethod':'password', 'Username':str(auth.user['username'])}, new_user['user_id'], int(mdserver_id) )
                        except MDMetadataServerError, e:
                            # could not apply changes to the server
                            rc = -1
                        
                        if rc < 0:
                            new_vars.update(dict(error=ENOMDCONTACT))
                        
                        redirect(URL(r=request, f='volumes', vars=new_vars))
                        
        
    return dict(form=volume_form)
    
    
    
@auth.requires_login()
def contents():
    """
    Control panel to manage a user's Syndicate volumes
    """
    
    global SMDS_api
    global error_msgs
    
    auth = current.session.auth
    
    api = SMDS_api
    content_form = FORM(_name="content_form")
    vars = request.vars
    new_vars = {}
        
    # do we have an error message?
    err = request.vars.get('error',None)
    try:
       err = int(err)
    except:
       pass

    if err and error_msgs.get(err) != None:
        content_form.append( H3("ERROR: %s" % error_msgs.get(err), _style="color:#EF0000") )
    
        
    # build up a table of the user's content servers
    if len(auth.user['content_ids']) == 0:
        content_form.append( H3("No Content Servers Registered") )
    else:
        content_table = build_content_table( api, auth.user )
        content_form.append( content_table )
     
    content_form.append( INPUT(_type="submit", _name="add content", _value="Add Content Server...") )
    
    if content_form.accepts( request.vars, session, formname="content_form" ):

        for v in request.vars.keys():
            
            if is_submit(request, v, "add content", "Add Content Server..."):
                # create a new volume
                redirect(URL(r=request, f='add_content', vars={}))
                
            elif v.startswith("delete_") and request.vars.get(v) == "Delete":
                # delete this content server
                content_id = -1
                try:
                    content_id = int(v.split("_")[1])
                except:
                    pass
                
                if content_id > 0:
                   rc = remove_content( api, auth.user, content_id )
                   if rc < 0:
                      new_vars.update( dict(error=ECDNAPI) )
                
                   redirect(URL(r=request, f='contents', vars=new_vars))
                   
                pass
            
        
    return dict(form=content_form)
    
    


@auth.requires_login()
def create_mdserver( api, user, vars ):
    smds_auth = {'AuthMethod':'password', 'Username':str(user['username'])}
    
    update_fields = {
        'auth_read': False,
        'auth_write': False
    }
    
    for (varname, varval) in vars.items():
        if is_mdserver_var( varname ):
            key = mdserver_key( varname )
            if key in MDServer.fields.keys():
                param_type = MDServer.fields[ key ].type
                val = param_type( varval )
                update_fields[key] = val
    
    server_id = -1
    createVolume = AddMetadataServer.AddMetadataServer( api )
    createVolume.caller = user
    try:
        server_id = createVolume.call( smds_auth, update_fields )
    except MDMetadataServerError, e:
        # could not contact
        server_id = -ENOMDCONTACT
    
    return server_id
    
    
@auth.requires_login()
def register_content( api, user, vars ):
    smds_auth = {'AuthMethod':'password', 'Username':str(user['username'])}
    
    update_fields = {}
    
    for (varname, varval) in vars.items():
        if is_content_var( varname ):
            key = content_key( varname )
            if key in Content.fields.keys():
                param_type = Content.fields[ key ].type
                val = param_type( varval )
                update_fields[key] = val
    
    content_id = -1
    addContent = AddContentServer.AddContentServer( api )
    addContent.caller = user
    try:
        content_id = addContent.call( smds_auth, update_fields )
    except Exception, e:
        traceback.print_exc()
        # could not register
        content_id = -ECDNAPI
    
    return content_id



@auth.requires_login()
def remove_content( api, user, content_id ):
    smds_auth = {'AuthMethod':'password', 'Username':str(user['username'])}
    
    rc = -1
    rmContent = DeleteContentServer.DeleteContentServer( api )
    rmContent.caller = user
    try:
        rc = rmContent.call( smds_auth, content_id )
    except Exception, e:
        traceback.print_exc()
        # could not register
        rc = -ECDNAPI
    
    return rc



@auth.requires_login()
def remove_mdserver( api, user, server_id ):
    smds_auth = {'AuthMethod':'password', 'Username':str(user['username'])}
    
    rc = -1
    rmServer = DeleteMetadataServer.DeleteMetadataServer( api )
    rmServer.caller = user
    try:
        rc = rmServer.call( smds_auth, server_id )
    except Exception, e:
        traceback.print_exc()
        # could not register
        rc = -ENOMDCONTACT
    
    return rc


@auth.requires_login()
def create_volume():
    """
    Control panel for creating a new volume
    """
    global SMDS_api
    global error_msgs
    
    auth = current.session.auth
    
    api = SMDS_api
    volume_form = FORM(_name="create_volume")
    vars = request.vars
    new_vars = {}
    
    # error?
    err = None
    if vars.get("error",None) != None:
        err = vars.get('error')
        try:
            err = int(err)
        except:
            pass
    
    if err:
        volume_form.append( H3("ERROR: %s" % error_msgs.get(err), _style="color:#EF0000") )
    
    avail_vols = api.all_hosts()
    
    volume_form.append(
        TABLE(
            TR(TD( B("Volume Name") ), TD(INPUT(_type="text", _maxlength="128", _style="display:table-cell", _name=mdserver_form_name("name"),
                                                _value="", requires=SMDS_validators.IS_FREE_VOLUME_NAME()))),
            TR(TD( B("Host") ),        TD(SELECT(avail_vols, _name=mdserver_form_name("host"), requires=IS_IN_SET(avail_vols))) ),
            TR(TD( B("Port Number") ), TD(INPUT(_type="text", _maxlength="5", _name=mdserver_form_name("portnum"), requires=IS_INT_IN_RANGE(1025, 65534)))),
            TR(TD( B("Authenticate Reads")), TD(INPUT(_type="checkbox", _name=mdserver_form_name("auth_read"), value="on")) ),
            TR(TD( B("Authenticate Writes")),TD(INPUT(_type="checkbox", _name=mdserver_form_name("auth_write"), value="on")) ),
        ))
    
    volume_form.append(INPUT(_type="submit", _name="create", _value="Create"))
    
    if volume_form.accepts( request.vars, session, formname="create_volume" ):

        for v in request.vars.keys():
            if is_submit( request, v, "create", "Create" ):
                user = auth.user
                server_id = create_mdserver( api, user, request.vars )
                if server_id < 0:
                    err = -server_id
                    new_vars.update(dict(error=err))
                    redirect(URL(r=request, f='create_volume', vars=new_vars) )
                    break
                else:
                    User.refresh( api, auth.user) 
                    redirect(URL(r=request, f='volumes', vars={}))
    
    
    return dict(form=volume_form)
    
    
    
@auth.requires_login()
def add_content():
    """
    Control panel for creating a new volume
    """
    global SMDS_api
    global error_msgs
    
    auth = current.session.auth
    
    api = SMDS_api
    content_form = FORM(_name="add_content")
    vars = request.vars
    new_vars = {}
    
    # error?
    err = None
    if vars.get("error",None) != None:
        err = vars.get('error')
        try:
            err = int(err)
        except:
            pass
    
    if err:
        content_form.append( H3("ERROR: %s" % error_msgs.get(err), _style="color:#EF0000") )
    
    
    content_form.append( 
      TABLE(
         TR(TD( B("Content Server URL") ), TD(INPUT(_type="text", _name=content_form_name("host_url"), requires=SMDS_validators.IS_HOSTNAME()) ))
      ))
   
    content_form.append( INPUT(_type="submit", _name="add", _value="Add") )
    
    
    if content_form.accepts( request.vars, session, formname="add_content" ):

        for v in request.vars.keys():
            if is_submit( request, v, "add", "Add" ):
                user = auth.user
                content_id = register_content( api, user, request.vars )
                if content_id < 0:
                    err = -content_id
                    new_vars.update(dict(error=err))
                    redirect(URL(r=request, f='add_content', vars=new_vars) )
                    break
                else:
                    User.refresh( api, auth.user) 
                    redirect(URL(r=request, f='contents', vars={}))
    
    
    
    return dict(form=content_form)
    
    

def build_metadata_server_table( api, mdserver_ids, td_func = None ):
    # NOTE: td_func generates more form data for the row.
    # it takes a metadata server object as its sole argument, and
    # returns the form data.
    mdserver_table = TABLE()
    if mdserver_ids:
        mdservers = MDServers( api, mdserver_ids )
        for mdserver in mdservers:
            row=TR()
            row.append( TD(mdserver['name']) )
            if td_func != None:
                # generate the rest of the row
                row.append( TD(td_func(mdserver)) )
                
            mdserver_table.append(row)
         
    else:
        mdserver_table.append(TR(TD("None")))
    
    return mdserver_table
    


def build_user_account_table( api, user ):
    # build up a contents table
    content_servers = TABLE()
    user_content_server_ids = user.get('content_ids')
    contents = []
    if user_content_server_ids:
        contents = Contents( api, user_content_server_ids )
    
    if contents:
        for content in contents:
            content_servers.append( TR( TD(content['host_url']) ) )
        
    # build up a list of owned metadata servers
    user_mdservers = build_metadata_server_table( api, user.get('my_mdserver_ids'), 
                                                lambda mdserver: INPUT(_type="submit", _value="Edit...", _name=edit_button_name(mdserver['server_id']) ) )

    sub_mdserver_ids = list( set(user.get('sub_mdserver_ids',[])).difference( set(user.get('my_mdserver_ids',[])) ) )
    sub_mdservers = build_metadata_server_table( api, sub_mdserver_ids )
    
    my_table = TABLE()
    my_table.append( TR( TD(B("Username")), TD(user['username']) ) )
    my_table.append( TR( TD(B("Email")), TD(user['email']) ) )
    my_table.append( TR( TD(B("Roles")), TD(", ".join(user['roles']) ) ) )
    my_table.append( TR( TD(B("Maximum Content Servers")), TD(user['max_contents']) ) )
    my_table.append( TR( TD(B("Maximum Volumes")), TD(user['max_mdservers']) ) )
    my_table.append( TR( TD(B("My Content Servers")), TD(content_servers) ) )
    my_table.append( TR( TD(B("My Volumes")), TD(user_mdservers) ) )
    
    if sub_mdserver_ids:
        my_table.append( TR( TD(B("Additional Subscribed Volumes")), TD(sub_mdservers) ) )
    
    return my_table
     

@auth.requires_login()
def my_account():
    """
    Control panel for viewing/updating my user
    """
    
    global SMDS_api
    global error_msgs
    
    auth = current.session.auth
    
    api = SMDS_api
    vars = request.vars
    new_vars = {}
    
    my_form = FORM(_name="my_form")
    my_form.append( build_user_account_table( SMDS_api, auth.user ) )
    my_form.append( INPUT(_type="submit", _name="editaccount", _value="Edit Details...") )
    my_form.append( INPUT(_type="submit", _name="resetpasswd", _value="Reset Password...") )
    
    if my_form.accepts( request.vars, session, formname="my_form" ):
        for v in request.vars.keys():
            if is_edit( request, v, edit="Edit..." ):
                if edit_server_redirect( request, v, new_vars ):
                    break
                 
            elif is_submit( request, v, "resetpasswd", "Reset Password..." ):
                pass
    
    return dict(form=my_form)

def error():
    return dict()
