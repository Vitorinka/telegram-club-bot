import json
import subprocess
import unittest
from pathlib import Path


APP_JS = Path(__file__).resolve().parents[1] / "miniapp" / "app.js"
INDEX_HTML = APP_JS.with_name("index.html")
STYLES_CSS = APP_JS.with_name("styles.css")


class ContentStudioFrontendTests(unittest.TestCase):
    def test_admin_shell_search_notifications_profile_and_navigation_contract(self):
        html = INDEX_HTML.read_text()
        source = APP_JS.read_text()
        styles = STYLES_CSS.read_text()
        for element_id in (
            "admin-global-search", "admin-search-results", "admin-notifications",
            "admin-notification-panel", "admin-profile-toggle", "admin-profile-panel",
            "admin-profile-panel-avatar", "admin-content-nav", "admin-content-subnav", "sidebar-open-club",
            "analytics-screen", "notifications-screen", "settings-screen",
        ):
            self.assertEqual(html.count(f'id="{element_id}"'), 1)
        self.assertIn('window.setTimeout(runAdminGlobalSearch,250)', source)
        self.assertIn('event.key === "Escape"', source)
        self.assertIn('adminNotificationReadKeys()', source)
        self.assertIn('configureAdminProfile(identityData)', source)
        self.assertIn('adminContentNav.setAttribute("aria-expanded"', source)
        self.assertIn('typeof webApp.requestFullscreen !== "function"', source)
        self.assertIn('data-nav="analytics"', html)
        self.assertIn('<small>Аналитика</small>', html)
        self.assertIn('data-nav="notifications"', html)
        self.assertIn('<small>Уведомления</small>', html)
        self.assertNotIn('id="nav-failed-subscriptions"', html)
        self.assertIn('button.dataset.nav === "analytics") loadAnalytics().catch(showApiError)', source)
        self.assertIn('button.dataset.nav === "notifications") loadNotifications().catch(showApiError)', source)
        self.assertIn('/api/admin/analytics?days=${encodeURIComponent(days)}', source)
        self.assertIn('/api/admin/content/cms?status=all&limit=50', source)
        self.assertIn('Контент · 50 последних', source)
        self.assertNotIn('50 последних материалов', html)
        self.assertIn('const adminNotificationReadState = new Set()', source)
        self.assertNotIn('localStorage', source)
        self.assertNotIn('sessionStorage', source)
        self.assertNotIn('document.cookie', source)
        self.assertIn('image.addEventListener("error",()=>{ element.replaceChildren(); element.textContent=profile.initials; }', source)
        self.assertIn('body:not(.member-preview-mode) .admin-global-search', styles)
        self.assertIn('body:not(.member-preview-mode) .admin-content-subnav', styles)
        self.assertNotIn('body.member-preview-mode .admin-', styles)

    def test_admin_shell_search_matching_and_notification_read_state(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          const items=[{key:'failed:1'},{key:'gift:2'},{key:'system:3'}];
          console.log(JSON.stringify({
            title:core.adminSearchMatches(['Утренняя медитация','lesson'],'медит'),
            category:core.adminSearchMatches(['Рецепт','Питание'],'пит'),
            tooShort:core.adminSearchMatches(['Рецепт'],'р'),
            unread:core.adminNotificationUnreadCount(items,new Set(['gift:2'])),
          }));
        """)
        self.assertEqual(result, {
            "title": True, "category": True, "tooShort": False, "unread": 2,
        })

    def test_notification_acknowledgements_hydrate_on_reopen_and_failed_write_stays_unread(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const reopened=new Set();
            core.hydrateAdminNotificationReadState(reopened,['failed:one']);
            const failed=new Set(); let rejected=false;
            try {
              await core.persistAdminNotificationRead({key:'gift:two',readState:failed,persist:async()=>{throw new Error('offline');}});
            } catch (_) { rejected=true; }
            const saved=new Set();
            const current=[{key:'delivery:three'},{key:'delivery:four'}];
            const before=core.adminNotificationUnreadCount(current,saved);
            await core.persistAdminNotificationRead({key:'delivery:three',readState:saved,persist:async()=>({ok:true})});
            const after=core.adminNotificationUnreadCount(current,saved);
            console.log(JSON.stringify({reopened:[...reopened],failed:[...failed],saved:[...saved],rejected,before,after}));
          })();
        """)
        self.assertEqual(result, {
            "reopened": ["failed:one"], "failed": [],
            "saved": ["delivery:three"], "rejected": True,
            "before": 2, "after": 1,
        })

    def test_system_notification_keys_track_incident_identity(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          console.log(JSON.stringify({
            schedulerSame:[
              core.adminSystemIncidentKey('scheduler','job:abc'),
              core.adminSystemIncidentKey('scheduler','job:abc'),
            ],
            schedulerNew:core.adminSystemIncidentKey('scheduler','job:def'),
            removalSame:[
              core.adminSystemIncidentKey('removals','removal:abc'),
              core.adminSystemIncidentKey('removals','removal:abc'),
            ],
            removalNew:core.adminSystemIncidentKey('removals','removal:def'),
          }));
        """)
        self.assertEqual(result["schedulerSame"], [
            "system:scheduler:job:abc", "system:scheduler:job:abc",
        ])
        self.assertEqual(result["schedulerNew"], "system:scheduler:job:def")
        self.assertEqual(result["removalSame"], [
            "system:removals:removal:abc", "system:removals:removal:abc",
        ])
        self.assertEqual(result["removalNew"], "system:removals:removal:def")
        source = APP_JS.read_text()
        for stable_key in (
            '`failed:${item.operation_id}`',
            '`delivery:${item.delivery_id}`',
            '`gift:${item.gift_id}`',
        ):
            self.assertIn(stable_key, source)

    def test_notification_pagination_counts_all_current_incidents(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const incidents=Array.from({length:30},(_,index)=>({operation_id:String(index+1)}));
            const first={items:incidents.slice(0,25),has_more:true,next_cursor:'page-2'};
            const full=await core.collectAdminNotificationKeys({
              firstPage:first,aggregate:30,keyFor:(item)=>`failed:${item.operation_id}`,
              fetchNext:async(cursor)=>({items:incidents.slice(25),has_more:false,next_cursor:null}),
            });
            const panel=incidents.slice(0,25).map((item)=>({key:`failed:${item.operation_id}`}));
            const noneRead=new Set(); const firstRead=new Set(panel.map((item)=>item.key));
            const allRead=new Set(full.keys); const historical=new Set(['failed:resolved']);
            console.log(JSON.stringify({
              size:full.keys.size,details:full.items.length,panel:panel.length,overflow:core.adminNotificationOverflowCount(full.keys.size,panel),
              unreadNone:[...full.keys].filter((key)=>!noneRead.has(key)).length,
              unreadFirst:[...full.keys].filter((key)=>!firstRead.has(key)).length,
              unreadAll:[...full.keys].filter((key)=>!allRead.has(key)).length,
              unreadHistorical:[...full.keys].filter((key)=>!historical.has(key)).length,
            }));
          })();
        """)
        self.assertEqual(result, {
            "size": 30, "details": 30, "panel": 25, "overflow": 5,
            "unreadNone": 30, "unreadFirst": 5,
            "unreadAll": 0, "unreadHistorical": 30,
        })

    def test_notification_pagination_supports_gifts_and_deliveries(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const collect=async(prefix,field)=>core.collectAdminNotificationKeys({
              firstPage:{items:Array.from({length:50},(_,i)=>({[field]:String(i)})),has_more:true,next_cursor:'next'},
              aggregate:55,keyFor:(item)=>`${prefix}:${item[field]}`,
              fetchNext:async()=>({items:Array.from({length:5},(_,i)=>({[field]:String(i+50)})),has_more:false}),
            });
            const gifts=await collect('gift','gift_id'); const deliveries=await collect('delivery','delivery_id');
            console.log(JSON.stringify({gifts:gifts.keys.size,deliveries:deliveries.keys.size,giftComplete:gifts.complete,deliveryComplete:deliveries.complete}));
          })();
        """)
        self.assertEqual(result, {
            "gifts": 55, "deliveries": 55,
            "giftComplete": True, "deliveryComplete": True,
        })

    def test_notification_pagination_fails_closed_on_bad_cursor_bound_and_error(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const keyFor=(item)=>`failed:${item.operation_id}`;
            const missing=await core.collectAdminNotificationKeys({firstPage:{items:[{operation_id:'1'}],has_more:true},aggregate:30,keyFor,fetchNext:async()=>{throw new Error('unused');}});
            let repeatedCalls=0;
            const repeated=await core.collectAdminNotificationKeys({firstPage:{items:[{operation_id:'1'}],has_more:true,next_cursor:'same'},aggregate:30,keyFor,fetchNext:async()=>{repeatedCalls++;return {items:[{operation_id:'2'}],has_more:true,next_cursor:'same'};}});
            const failed=await core.collectAdminNotificationKeys({firstPage:{items:[{operation_id:'1'}],has_more:true,next_cursor:'next'},aggregate:30,keyFor,fetchNext:async()=>{throw new Error('offline');}});
            const bounded=await core.collectAdminNotificationKeys({firstPage:{items:[{operation_id:'1'}],has_more:true,next_cursor:'next'},aggregate:30,keyFor,maxPages:1,fetchNext:async()=>({items:[],has_more:false})});
            console.log(JSON.stringify({
              missing:[missing.complete,core.adminNotificationUnknownUnread(missing)],
              repeated:[repeated.complete,repeatedCalls,core.adminNotificationUnknownUnread(repeated)],
              failed:[failed.complete,core.adminNotificationUnknownUnread(failed)],
              bounded:[bounded.complete,core.adminNotificationUnknownUnread(bounded)],
            }));
          })();
        """)
        self.assertEqual(result, {
            "missing": [False, 29], "repeated": [False, 1, 28],
            "failed": [False, 29], "bounded": [False, 29],
        })

    def test_notification_panel_progressively_exposes_all_deduplicated_current_items(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const raw=Array.from({length:60},(_,index)=>({
              operation_id:String(index+1),
              timestamp:new Date(Date.UTC(2026,8,20,12,0,index)).toISOString(),
            }));
            const collected=await core.collectAdminNotificationKeys({
              firstPage:{items:[...raw.slice(0,50),raw[0]],has_more:true,next_cursor:'page-2'},
              aggregate:60,keyFor:(item)=>`failed:${item.operation_id}`,
              fetchNext:async()=>({items:raw.slice(50),has_more:false}),
            });
            const mixed=[
              ...collected.items.map((item)=>({key:`failed:${item.operation_id}`,timestamp:item.timestamp})),
              {key:'gift:new',timestamp:'2026-09-20T12:02:00Z'},
              {key:'delivery:new',timestamp:'2026-09-20T12:01:00Z'},
              {key:'system:scheduler:run-1'},
              {key:'gift:new',timestamp:'2026-09-20T12:02:00Z'},
            ];
            const ordered=core.orderAdminNotificationItems(mixed);
            const first=core.adminNotificationPanelPage(ordered,25);
            const second=core.adminNotificationPanelPage(ordered,50);
            const final=core.adminNotificationPanelPage(ordered,75);
            const read=new Set(ordered.slice(0,25).map((item)=>item.key));
            const unreadAfterFirst=[...collected.keys].filter((key)=>!read.has(key)).length;
            console.log(JSON.stringify({
              collected:collected.items.length,unique:ordered.length,
              first:[first.visible.length,first.remaining],second:[second.visible.length,second.remaining],final:[final.visible.length,final.remaining],
              leading:ordered.slice(0,2).map((item)=>item.key),unreadAfterFirst,
              systemReachable:final.visible.some((item)=>item.key==='system:scheduler:run-1'),
            }));
          })();
        """)
        self.assertEqual(result, {
            "collected": 60, "unique": 63,
            "first": [25, 38], "second": [50, 13], "final": [63, 0],
            "leading": ["gift:new", "delivery:new"], "unreadAfterFirst": 37,
            "systemReachable": True,
        })

    def test_notification_panel_incomplete_state_and_load_more_are_non_mutating(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            let fetches=0;
            const result=await core.collectAdminNotificationKeys({
              firstPage:{items:[{operation_id:'1'},{operation_id:'2'}],has_more:true,next_cursor:'next'},
              aggregate:5,keyFor:(item)=>`failed:${item.operation_id}`,
              fetchNext:async()=>{fetches++;throw new Error('offline');},
            });
            const items=core.orderAdminNotificationItems(result.items.map((item)=>({key:`failed:${item.operation_id}`})));
            const before=JSON.stringify(items); const page=core.adminNotificationPanelPage(items,25); const after=JSON.stringify(items);
            console.log(JSON.stringify({complete:result.complete,loaded:page.visible.length,unknown:core.adminNotificationUnknownUnread(result),fetches,unchanged:before===after}));
          })();
        """)
        self.assertEqual(result, {
            "complete": False, "loaded": 2, "unknown": 3,
            "fetches": 1, "unchanged": True,
        })
        source = APP_JS.read_text()
        self.assertIn('Не все уведомления удалось загрузить.', source)
        self.assertIn('Показать ещё ${Math.min(MAX_NOTIFICATION_PANEL_ITEMS,page.remaining)}', source)
        self.assertNotIn('событий требуют просмотра', source)

    def test_admin_profile_uses_safe_telegram_photo_with_initials_fallback(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          console.log(JSON.stringify({
            photo:core.adminProfilePresentation({first_name:'Natalia',last_name:'SoulFit',photo_url:'https://t.me/i/userpic/320/avatar.jpg'}),
            missing:core.adminProfilePresentation({first_name:'Natalia',last_name:'SoulFit'}),
            unsafe:core.adminProfilePresentation({first_name:'Natalia',photo_url:'https://cdn.example/avatar.jpg'}),
          }));
        """)
        self.assertEqual(result["photo"], {
            "displayName": "Natalia SoulFit", "initials": "NS",
            "photoUrl": "https://t.me/i/userpic/320/avatar.jpg",
        })
        self.assertEqual(result["missing"]["initials"], "NS")
        self.assertIsNone(result["missing"]["photoUrl"])
        self.assertEqual(result["unsafe"]["initials"], "N")
        self.assertIsNone(result["unsafe"]["photoUrl"])
        source = APP_JS.read_text()
        self.assertIn("photo_url: confirmedProfile.photo_url || null", source)
        self.assertNotIn("upload avatar", source.lower())

    def test_direct_upload_recovers_lost_response_without_second_put_or_media_record(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            let now=1000000, creates=0, puts=0, finalizes=0, objectExists=false, retained=null;
            const common={
              createIntent:async()=>{creates++;return {upload_id:'u1',upload_url:'https://r2.test/u1',expires_in:300};},
              putObject:async()=>{puts++;objectExists=true;throw new Error('object_upload_failed');},
              finalize:async()=>{finalizes++;if(!objectExists)throw new Error('object_storage_object_missing');return {status:'completed',media_id:'m1'};},
              onIntent:(intent)=>{retained=intent;},
              now:()=>now,
            };
            let firstFailed=false;
            try { await core.runContentMediaDirectUpload(common); } catch (_) { firstFailed=true; }
            const recovered=await core.runContentMediaDirectUpload({...common,intent:retained,putObject:async()=>{puts++;}});
            console.log(JSON.stringify({firstFailed,creates,puts,finalizes,recovered:recovered.recovered,retained:retained.upload_id}));
          })();
        """)
        self.assertEqual(result, {
            "firstFailed": True, "creates": 1, "puts": 1,
            "finalizes": 1, "recovered": True, "retained": "u1",
        })

    def test_direct_upload_rotates_expired_intent_and_cancels_old_upload_row(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            let active=null,creates=0,puts=0,finalizes=0,cancels=[];
            const expired={upload_id:'old',upload_url:'https://r2.test/old',expires_in:60,client_received_at:1000};
            const outcome=await core.runContentMediaDirectUpload({
              intent:expired,now:()=>100000,
              createIntent:async()=>{creates++;return {upload_id:'new',upload_url:'https://r2.test/new',expires_in:300};},
              putObject:async(intent)=>{puts++;active=intent.upload_id;},
              finalize:async(intent)=>{finalizes++;if(intent.upload_id==='old')throw new Error('object_storage_object_missing');return {status:'completed',media_id:'m2'};},
              cancelIntent:async(intent)=>{cancels.push(intent.upload_id);},
              onIntent:(intent)=>{active=intent&&intent.upload_id;},
            });
            console.log(JSON.stringify({creates,puts,finalizes,cancels,active,result:outcome.result.status}));
          })();
        """)
        self.assertEqual(result, {
            "creates": 1, "puts": 1, "finalizes": 2,
            "cancels": ["old"], "active": "new", "result": "completed",
        })

    def test_upload_gate_generation_and_post_finalize_refresh_are_behavioral(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const gate=core.createContentMediaOperationGate();
            const first={}; const second={};
            const firstStarted=gate.begin(first);
            const duplicateStarted=gate.begin(second);
            gate.end(second);
            const stillLocked=gate.isActive();
            gate.end(first);
            const secondAfterFinish=gate.begin(second);
            const current=core.contentMediaOperationIsCurrent({
              operationGeneration:2,operationContentId:'a',currentGeneration:2,currentContentId:'a',
            });
            const switched=core.contentMediaOperationIsCurrent({
              operationGeneration:2,operationContentId:'a',currentGeneration:3,currentContentId:'b',
            });
            let refreshCalls=0;
            const refresh=await core.refreshContentMediaAfterFinalize(async()=>{refreshCalls++;throw new Error('offline');});
            console.log(JSON.stringify({firstStarted,duplicateStarted,stillLocked,secondAfterFinish,current,switched,refreshCalls,saved:refresh.saved,synchronized:refresh.synchronized}));
          })();
        """)
        self.assertEqual(result, {
            "firstStarted": True, "duplicateStarted": False,
            "stillLocked": True, "secondAfterFinish": True,
            "current": True, "switched": False, "refreshCalls": 1,
            "saved": True, "synchronized": False,
        })

    def test_admin_navigation_behavior_matches_real_dom_shell(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          console.log(JSON.stringify({
            editor:core.adminShellNavigationVisible('content-details'),
            create:core.adminShellNavigationVisible('content-create'),
            list:core.adminShellNavigationVisible('content'),
          }));
        """)
        self.assertEqual(result, {"editor": True, "create": True, "list": True})
        html = INDEX_HTML.read_text()
        styles = STYLES_CSS.read_text()
        source = APP_JS.read_text()
        self.assertLess(html.index('<main class="shell"'), html.index('<nav id="bottom-nav"'))
        self.assertIn('bottomNav.classList.toggle("editor-navigation-hidden"', source)
        self.assertIn('> #bottom-nav.editor-navigation-hidden', styles)
        self.assertNotIn('#content-create:not([hidden])) > .bottom-nav', styles)

    def test_figma_content_studio_replaces_legacy_shell_without_losing_cms_hooks(self):
        html = INDEX_HTML.read_text()
        styles = STYLES_CSS.read_text()
        source = APP_JS.read_text()
        self.assertNotIn('class="card studio-legacy"', html)
        self.assertNotIn('class="studio-create-options"', html)
        self.assertIn('class="studio-table-head"', html)
        self.assertIn('class="studio-create-layout"', html)
        self.assertIn('class="studio-current-summary"', html)
        self.assertIn('data-status-count="published"', html)
        self.assertIn('renderContentStatusCount', source)
        self.assertIn('renderCreateContentPreview', source)
        self.assertIn('Figma nodes 11:162, 12:2, 221:1805, 141:377, 141:425, 223:315', styles)

    def test_figma_editor_keeps_single_authoritative_media_state_machine(self):
        html = INDEX_HTML.read_text()
        source = APP_JS.read_text()
        self.assertEqual(html.count('id="content-media-confirmation"'), 1)
        self.assertEqual(html.count('id="content-media-confirm"'), 1)
        self.assertEqual(html.count('id="content-media-progress"'), 1)
        for stage in ("selected", "uploading", "verifying", "saved", "failed"):
            self.assertIn(f'{stage}: "', source)
        self.assertIn('uploadContentMediaDirect(', source)
        self.assertIn('/media/uploads/${encodeURIComponent(intent.upload_id)}/finalize', source)

    def test_admin_screen_model_has_exactly_one_visible_screen(self):
        output = self.run_node("""
          const core=require('./miniapp/app.js');
          const screens=['overview','content','users','system'];
          const result={};
          for (const active of ['users','content','system']) {
            result[active]=screens.filter((screen)=>core.adminScreenIsVisible(active,screen));
          }
          console.log(JSON.stringify(result));
        """)
        self.assertEqual(output, {
            "users": ["users"], "content": ["content"], "system": ["system"],
        })

    def test_member_video_source_preserves_authenticated_telegram_and_direct_r2_paths(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            const calls=[]; const created=[];
            const telegram=await core.memberVideoSource({
              media:{storage_kind:'telegram_file_id',mime_type:'video/mp4',url:'/api/member/content/c/media/m'},
              sessionToken:'member-session',
              fetchMedia:async(url,options)=>{
                calls.push({url,options});
                return {ok:true,status:200,blob:async()=>({type:'video/mp4',bytes:12})};
              },
              createObjectUrl:(blob)=>{created.push(blob);return 'blob:telegram-video';},
            });
            let r2Fetches=0;
            const r2=await core.memberVideoSource({
              media:{storage_kind:'r2',mime_type:'video/webm',url:'https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/private/signed'},
              sessionToken:'member-session',
              fetchMedia:async()=>{r2Fetches+=1;throw new Error('must_not_fetch');},
              createObjectUrl:()=>{throw new Error('must_not_create_blob');},
            });
            console.log(JSON.stringify({telegram,r2,calls,createdType:created[0].type,r2Fetches}));
          })();
        """)
        self.assertEqual(result["telegram"], {"src": "blob:telegram-video", "objectUrl": "blob:telegram-video"})
        self.assertEqual(result["calls"], [{
            "url": "/api/member/content/c/media/m",
            "options": {
                "headers": {"Authorization": "Bearer member-session"},
                "cache": "no-store", "credentials": "omit",
            },
        }])
        self.assertEqual(result["createdType"], "video/mp4")
        self.assertEqual(result["r2"], {
            "src": "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/private/signed",
            "objectUrl": None,
        })
        self.assertEqual(result["r2Fetches"], 0)

    def test_member_video_source_rejects_unauthorized_telegram_media(self):
        result = self.run_node(r"""
          const core=require('./miniapp/app.js');
          (async()=>{
            let blobs=0,objects=0,status=null;
            try {
              await core.memberVideoSource({
                media:{storage_kind:'telegram_file_id',mime_type:'video/mp4',url:'/api/member/content/c/media/m'},
                sessionToken:'member-session',
                fetchMedia:async()=>({ok:false,status:403,blob:async()=>{blobs+=1;}}),
                createObjectUrl:()=>{objects+=1;return 'blob:forbidden';},
              });
            } catch (error) { status=error.status; }
            console.log(JSON.stringify({status,blobs,objects}));
          })();
        """)
        self.assertEqual(result, {"status": 403, "blobs": 0, "objects": 0})

    def test_member_video_cleanup_revokes_only_blob_object_urls(self):
        source = APP_JS.read_text()
        self.assertIn("if (memberVideoObjectUrl) URL.revokeObjectURL(memberVideoObjectUrl)", source)
        self.assertIn("memberVideoObjectUrl = source.objectUrl", source)
        self.assertNotIn("URL.revokeObjectURL(memberVideoUrl)", source)
        self.assertIn("if (source.objectUrl) URL.revokeObjectURL(source.objectUrl)", source)

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
            const oversized = {name:'lesson.mp4', type:'video/mp4', size:2*1024*1024*1024+1};
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

    def test_failed_media_keeps_the_exact_file_for_retry(self):
        result = self.run_node(r"""
          const core = require('./miniapp/app.js');
          (async()=>{
            const video={name:'lesson.mp4',type:'video/mp4',size:2048};
            const result=await core.contentStudioCreateDraft({
              files:[['cover',undefined],['video',video]],
              createDraft:async()=>({content_id:'draft'}),
              saveDomain:async()=>{},
              attachMedia:async(_draft,kind,file)=>{if(kind==='video' && file) throw new Error('offline');},
              openDraft:async()=>{},
            });
            console.log(JSON.stringify({status:result.status,kind:result.failedMedia.mediaType,same:result.failedMedia.file===video,name:result.failedMedia.file.name}));
          })();
        """)
        self.assertEqual(result, {
            "status": "media_failed", "kind": "video",
            "same": True, "name": "lesson.mp4",
        })

    def test_content_studio_selected_upload_and_local_preview_contract(self):
        html = INDEX_HTML.read_text()
        source = APP_JS.read_text()
        styles = STYLES_CSS.read_text()
        for element_id in (
            "content-create-video-selection", "content-video-selection",
            "content-media-progress",
        ):
            self.assertEqual(html.count(f'id="{element_id}"'), 1)
        self.assertIn('rememberPendingContentMedia(mediaType, file)', source)
        self.assertIn('Файл остаётся выбранным — повторите загрузку.', source)
        self.assertIn('contentMediaUploadGate.begin(operationToken)', source)
        self.assertIn('setContentUploadProgress(percent)', source)
        self.assertIn('onStage("verifying")', source)
        self.assertIn('intent: contentMediaDirectIntent', source)
        self.assertIn('contentMediaDirectIntent = intent', source)
        self.assertIn('contentVideoFile.addEventListener("change"', source)
        self.assertIn('validateContentMedia("video")', source)
        self.assertIn('id="content-video-validate" type="button" hidden', html)
        self.assertIn('Загрузить и сохранить', html)
        self.assertIn('get("preview") === "admin-content"', source)
        self.assertIn('.studio-upload-progress', styles)
        self.assertIn('.admin-content-local-preview::before', styles)
        self.assertIn('overflow-wrap:anywhere', styles)
        self.assertIn('grid-template-columns:minmax(0,1fr)', styles)

    def test_telegram_editor_uses_one_scroll_surface_without_overlapping_nav(self):
        styles = STYLES_CSS.read_text()
        source = APP_JS.read_text()
        self.assertIn('#content-details .studio-toolbar', styles)
        self.assertIn('position: static', styles)
        self.assertIn('> #bottom-nav.editor-navigation-hidden', styles)
        self.assertNotIn('#content-create:not([hidden])) > .bottom-nav', styles)
        self.assertIn('#content-details .studio-editor-actions', styles)
        self.assertIn('display: none !important', styles)
        self.assertIn('--tg-viewport-height', source)
        self.assertIn('viewportChanged', source)

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
