"""Exercise deployment orchestration locally; this is not a Docker integration test."""
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

FAKE = r'''#!/usr/bin/env python3
import json,os,sys,tarfile,shutil,re
from pathlib import Path
root=Path(os.environ['FAKE_ROOT']);statefile=root/'state.json';s=json.loads(statefile.read_text());args=sys.argv[1:];name=Path(sys.argv[0]).name
with (root/'commands').open('a') as f:f.write(json.dumps([name]+args)+'\n')
def save():statefile.write_text(json.dumps(s))
if name in ('chown','sleep'):sys.exit(0)
if name=='curl':sys.exit(1 if s.get('generation')=='new' and os.environ['FAIL_NEW']=='1' else 0)
if args[0]=='inspect':
 if '-f' not in args:sys.exit(0 if s.get('main') else 1)
 print('old-image-id' if '.Image' in args[2] else 'litepay-data');sys.exit(0)
if args[0]=='rename':
 if args[1]=='litepay':s.update(main=False,old=True)
 else:s.update(main=True,old=False,generation='old')
 save()
elif args[0]=='rm':
 if args[-1]=='litepay':s['main']=False
 else:s['old']=False
 save()
elif args[0]=='compose':
 if '-p' in args and not re.fullmatch('[a-z0-9][a-z0-9_-]*',args[args.index('-p')+1]):sys.exit(1)
 if 'create' in args or 'up' in args:
  if s.get('old') and '-p' not in args:sys.exit(1)
  s.update(main=True,generation='new');save()
elif args[0]=='run':
 cmd=' '.join(args);volume=root/'volume'
 if 'tar czf' in cmd:
  target=next(args[i+1].split(':')[0] for i,x in enumerate(args[:-1]) if x=='-v' and '/backup' in args[i+1])
  with tarfile.open(Path(target)/'litepay-data.tar.gz','w:gz') as t:t.add(volume,arcname='.')
 elif 'tar xzf' in cmd:
  backup=next(args[i+1].split(':')[0] for i,x in enumerate(args[:-1]) if x=='-v' and '/backup' in args[i+1])
  shutil.rmtree(volume);volume.mkdir()
  with tarfile.open(Path(backup)/'litepay-data.tar.gz') as t:t.extractall(volume)
 elif 'touch /app/data/.deploy-gate' in cmd:(volume/'.deploy-gate').touch()
elif args[0]=='exec':
 if args[2:]==['id','-u']:print('10001')
 if 'rm' in args:(root/'volume'/'.deploy-gate').unlink()
'''

class DeploymentRecoveryTest(unittest.TestCase):
    def run_deployment(self, fail):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            bindir=root/'bin';bindir.mkdir()
            for command in ['docker','curl','chown','sleep']:
                path=bindir/command;path.write_text(FAKE);path.chmod(0o755)
            deploy=root/'release';deploy.mkdir()
            for name in ['litepay','config.yaml','Dockerfile','docker-compose.yml','.dockerignore']:
                (deploy/name).write_text('old '+name)
            volume=root/'volume';volume.mkdir();(volume/'litepay.db').write_text('old database')
            stage=root/'stage';stage.mkdir()
            for name in ['litepay','config.yaml','Dockerfile','docker-compose.yml','.dockerignore']:
                (stage/name).write_text('new '+name)
            archive=root/'package.tar.gz'
            with tarfile.open(archive,'w:gz') as tar:
                for file in stage.iterdir():tar.add(file,arcname=file.name)
            (root/'state.json').write_text(json.dumps(dict(main=True,old=False,generation='old')))
            env=dict(os.environ,PATH=str(bindir)+os.pathsep+os.environ['PATH'],FAKE_ROOT=str(root),FAIL_NEW=str(int(fail)),LP_HEALTH_ATTEMPTS='1',DEPLOY_PATH=str(deploy),ARCHIVE=str(archive))
            result=subprocess.run(['bash','scripts/deploy-activate.sh'],env=env,capture_output=True,text=True)
            state=json.loads((root/'state.json').read_text())
            self.assertTrue(state['main'],result.stderr)
            self.assertFalse((volume/'.deploy-gate').exists())
            backups=list(root.glob('release-backups/*/litepay-data.tar.gz'))
            self.assertEqual(len(backups),1)
            if fail:
                self.assertNotEqual(result.returncode,0)
                self.assertEqual(state['generation'],'old')
                self.assertEqual((deploy/'config.yaml').read_text(),'old config.yaml')
                self.assertEqual((volume/'litepay.db').read_text(),'old database')
                self.assertIn('Previous version restored',result.stderr)
            else:
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(state['generation'],'new')
                self.assertEqual((deploy/'config.yaml').read_text(),'new config.yaml')
    def test_failed_health_restores_previous_version(self):self.run_deployment(True)
    def test_verified_release_opens_gate_and_retains_backup(self):self.run_deployment(False)

if __name__=='__main__':unittest.main()
