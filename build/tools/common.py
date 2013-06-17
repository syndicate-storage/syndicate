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

# install a list of targets and set up aliases
def install_targets( env, alias, dir, targets ):
   flatten = lambda lst: reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple, SCons.Node.NodeList)) else l + [i], lst, [])
   targets = flatten( targets )

   for target in targets:
      bn = os.path.basename( target.path )
      bn_install_path = os.path.join( dir, bn )
      
      env.InstallProgram( dir, target )
      env.Alias( alias, bn_install_path )


