import os
import sys
import SCons

def setup_env( env ):
   # install a set of files with the given permissions
   from SCons.Script.SConscript import SConsEnvironment
   SConsEnvironment.Chmod = SCons.Action.ActionFactory(os.chmod, lambda dest, mode: 'Chmod("%s", 0%o)' % (dest, mode))

   def InstallPerm(env, dest, files, perm):
       obj = env.Install(dest, files)
       for i in obj:
           env.AddPostAction(i, env.Chmod(str(i), perm))
       return dest

   SConsEnvironment.InstallPerm = InstallPerm

   # installers for binaries, headers, and libraries
   SConsEnvironment.InstallProgram = lambda env, dest, files: InstallPerm(env, dest, files, 0755)
   SConsEnvironment.InstallHeader = lambda env, dest, files: InstallPerm(env, dest, files, 0644)
   SConsEnvironment.InstallLibrary = lambda env, dest, files: InstallPerm(env, dest, files, 0644)


# flatten targets 
def flatten_targets( targets ):
   flatten = lambda lst: reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple, SCons.Node.NodeList)) else l + [i], lst, [])
   targets = flatten( targets )
   return targets

# install a tree of targets and set up aliases 
def install_tree( env, alias, dir, targets, variant_dir ):
    targets = flatten_targets( targets )

    prefix = variant_dir.rstrip("/") + "/"

    for target in targets:
       if not target.path.startswith(prefix):
           raise Exception("Invalid target: %s is not built in %s" % (target.path, prefix))

       leaf_path = target.path.rstrip("/") 
       leaf_path = leaf_path[len(prefix):]

       leaf_path_install = os.path.join( dir, leaf_path )
       leaf_path_dir = os.path.join( dir, os.path.dirname(leaf_path))

       env.Install( leaf_path_dir, target )
       env.Alias( alias, leaf_path_install )


# install a list of targets and set up aliases
def install_targets( env, alias, dir, targets ):
   targets = flatten_targets( targets )

   for target in targets:
      bn = os.path.basename( target.path )
      bn_install_path = os.path.join( dir, bn )
      
      env.InstallProgram( dir, target )
      env.Alias( alias, bn_install_path )


