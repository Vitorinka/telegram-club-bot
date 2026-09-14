import json
import subprocess
import unittest
from pathlib import Path


APP_JS = Path(__file__).resolve().parents[1] / "miniapp" / "app.js"


class ContentStudioFrontendTests(unittest.TestCase):
    def run_node(self, source):
        completed = subprocess.run(
            ["node", "-e", source],
            cwd=APP_JS.parents[1],
            text=True,
            capture_output=True,
            check=True,
        )
        return json.loads(completed.stdout)

    def test_create_preflight_and_stage_failures_preserve_exact_draft(self):
        result = self.run_node(r"""
          const core = require('./miniapp/app.js');
          (async () => {
            let creates = 0, opens = [], attaches = 0;
            const oversized = {name:'lesson.mp4', type:'video/mp4', size:21*1024*1024};
            const blocked = await core.contentStudioCreateDraft({
              files:[['video', oversized]],
              createDraft:async()=>{ creates += 1; return {content_id:'unexpected'}; },
              saveDomain:async()=>{}, attachMedia:async()=>{}, openDraft:async()=>{},
            });
            const file = {name:'lesson.mp4', type:'video/mp4', size:1024};
            const failed = await core.contentStudioCreateDraft({
              files:[['video', file]],
              createDraft:async()=>{ creates += 1; return {content_id:'draft-1'}; },
              saveDomain:async()=>{},
              attachMedia:async()=>{ attaches += 1; throw new Error('upload_failed'); },
              openDraft:async(draft)=>{ opens.push(draft.content_id); },
            });
            const domain = await core.contentStudioCreateDraft({
              files:[],
              createDraft:async()=>{ creates += 1; return {content_id:'draft-2'}; },
              saveDomain:async()=>{ throw new Error('domain_failed'); },
              attachMedia:async()=>{ attaches += 1; },
              openDraft:async(draft)=>{ opens.push(draft.content_id); },
            });
            let createThrown=false, createOpens=0;
            try {
              await core.contentStudioCreateDraft({
                files:[], createDraft:async()=>{throw new Error('create_failed');},
                saveDomain:async()=>{}, attachMedia:async()=>{}, openDraft:async()=>{createOpens += 1;},
              });
            } catch (_) { createThrown=true; }
            console.log(JSON.stringify({blocked:blocked.status, failed:failed.status, domain:domain.status, creates, opens, attaches, id:failed.draft.content_id, domainId:domain.draft.content_id, createThrown, createOpens}));
          })();
        """)
        self.assertEqual(result, {
            "blocked": "preflight_failed", "failed": "media_failed",
            "domain": "domain_failed", "creates": 2,
            "opens": ["draft-1", "draft-2"], "attaches": 1,
            "id": "draft-1", "domainId": "draft-2",
            "createThrown": True, "createOpens": 0,
        })

    def test_recipe_reorder_and_combined_save_orchestration(self):
        result = self.run_node(r"""
          const core = require('./miniapp/app.js');
          (async () => {
            const items=['first','second'];
            let dirty=false;
            const moved=core.contentStudioMove(items,0,1,()=>{dirty=true;});
            const ignored=core.contentStudioMove(items,0,-1);
            const calls=[];
            await core.contentStudioSaveRecipe({
              saveMetadata:async()=>{calls.push('metadata'); return {version:2};},
              saveRecipe:async()=>{calls.push('recipe'); return {version:3};},
              reload:async()=>{calls.push('reload');},
            });
            console.log(JSON.stringify({moved,ignored,dirty,items,calls}));
          })();
        """)
        self.assertEqual(result, {
            "moved": True, "ignored": False, "dirty": True,
            "items": ["second", "first"],
            "calls": ["metadata", "recipe", "reload"],
        })

    def test_media_preflight_accepts_only_supported_bounded_files(self):
        result = self.run_node(r"""
          const core = require('./miniapp/app.js');
          const check=(kind,name,type,size)=>core.contentStudioMediaPreflightError(kind,{name,type,size});
          console.log(JSON.stringify({
            cover:check('cover','cover.webp','image/webp',10*1024*1024),
            badCover:check('cover','cover.gif','image/gif',10),
            video:check('video','lesson.mp4','video/mp4',20*1024*1024),
            audio:check('audio','calm.mp3','audio/mpeg',20*1024*1024),
          }));
        """)
        self.assertIsNone(result["cover"])
        self.assertIsNone(result["video"])
        self.assertIsNone(result["audio"])
        self.assertIn("JPEG", result["badCover"])

    def test_dirty_editor_blocks_media_start(self):
        result = self.run_node(r"""
          const core = require('./miniapp/app.js');
          console.log(JSON.stringify({clean:core.contentStudioCanStartMedia(false),dirty:core.contentStudioCanStartMedia(true)}));
        """)
        self.assertEqual(result, {"clean": True, "dirty": False})

    def test_stale_category_falls_back_and_only_cover_can_override_preview(self):
        result = self.run_node(r"""
          const core = require('./miniapp/app.js');
          const items=[{categories:[{id:'current'}]}];
          console.log(JSON.stringify({
            stale:core.contentStudioEffectiveCategory(items,'removed'),
            current:core.contentStudioEffectiveCategory(items,'current'),
            cover:core.contentStudioCoverUrl({localUrl:'blob:cover',localMediaType:'cover',attachedUrl:'blob:attached'}),
            video:core.contentStudioCoverUrl({localUrl:'blob:video',localMediaType:'video',attachedUrl:'blob:attached'}),
            audio:core.contentStudioCoverUrl({localUrl:'blob:audio',localMediaType:'audio',attachedUrl:'blob:attached'}),
          }));
        """)
        self.assertEqual(result, {
            "stale": "all", "current": "current",
            "cover": "blob:cover", "video": "blob:attached",
            "audio": "blob:attached",
        })


if __name__ == "__main__":
    unittest.main()
