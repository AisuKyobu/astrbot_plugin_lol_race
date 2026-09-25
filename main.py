import asyncio
import json
from datetime import timedelta, timezone
from urllib.parse import quote

import httpx

try:
    import websockets

    HAS_WS = True
except ImportError:
    HAS_WS = False
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

DEFAULT_API_BASE = "http://127.0.0.1:8000/api/v1"
DEFAULT_FRONTEND_URL = "http://127.0.0.1:5173"
TZ = timezone(timedelta(hours=8))  # Asia/Shanghai


@register("lolrace", "一只冰块", "LOL内战系统", "v1.0.0")
class LolRacePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config
        cfg = config or {}
        self.api_base = str(cfg.get("api_base") or DEFAULT_API_BASE).rstrip("/")
        self.frontend_url = str(
            cfg.get("frontend_url") or DEFAULT_FRONTEND_URL
        ).rstrip("/")
        self._group_origin = None
        logger.warning(f"[lolrace] plugin loaded, api_base={self.api_base}")
        self._ws_task = asyncio.create_task(self._ws_listener())

    async def terminate(self):
        """AstrBot 卸载/重载插件时取消后台任务，避免残留旧监听。"""
        task = getattr(self, "_ws_task", None)
        if task:
            task.cancel()
            logger.warning("[lolrace] ws listener terminated")

    async def _api(self, method: str, path: str, **kwargs) -> dict | list | None:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.request(method, f"{self.api_base}{path}", **kwargs)
                if r.status_code >= 500:
                    logger.warning(f"[lolrace] api {method} {path} -> {r.status_code}")
                data = r.json()
                logger.warning(f"[lolrace] api {method} {path} -> {data}")
                return data
        except httpx.ConnectError:
            logger.warning(f"[lolrace] api {method} {path} -> 无法连接到后端")
            return None
        except Exception as e:
            logger.warning(f"[lolrace] api {method} {path} error: {e}")
            return None

    def _api_err(self) -> str:
        return "❌ 后端服务暂不可用，请联系管理员"

    def _name(self, p: dict) -> str:
        return p.get("group_id") or p.get("game_name", "?")

    def _get_text(self, event: AstrMessageEvent) -> str:
        parts = []
        for comp in event.get_messages():
            if comp.type == "Plain":
                parts.append(comp.text)
        return "".join(parts).strip()

    def _record_origin(self, event: AstrMessageEvent):
        origin = getattr(event, "unified_msg_origin", None)
        if origin:
            self._group_origin = origin
            logger.warning(f"[lolrace] _record_origin updated _group_origin={origin}")

    async def _ws_listener(self):
        if not HAS_WS:
            logger.warning(
                "[lolrace] websockets not installed, remind listener disabled"
            )
            return
        while True:
            ws_url = (
                self.api_base.replace("https://", "wss://")
                .replace("http://", "ws://")
                .rstrip("/")
                + "/ws"
            )
            try:
                async with websockets.connect(ws_url, ping_interval=None) as ws:
                    logger.warning(f"[lolrace] ws connected: {ws_url}")
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        if data.get("channel") == "ping":
                            await ws.send(json.dumps("pong"))
                        elif data.get("channel") == "remind":
                            logger.warning(
                                f"[lolrace] ws got remind, gid={self._group_origin}, payload keys={list(data.get('payload', {}).keys())}"
                            )
                            await self._send_remind(data["payload"])
                        elif data.get("channel") == "bulletin":
                            logger.warning(
                                f"[lolrace] ws got bulletin, payload keys={list(data.get('payload', {}).keys())}"
                            )
                            await self._send_bulletin(data["payload"])
                        elif data.get("channel") == "auction_bot":
                            await self._send_auction_notify(data.get("payload") or {})
            except asyncio.CancelledError:
                logger.warning("[lolrace] ws listener cancelled")
                raise
            except Exception as e:
                logger.warning(f"[lolrace] ws disconnected: {e}, retry in 5s")
                await asyncio.sleep(5)

    async def _send_remind(self, payload: dict):
        group_id = str(payload.get("group_id") or "").strip()
        target_origin = self._origin_for_group(group_id) or self._group_origin
        if not target_origin:
            logger.warning("[lolrace] remind: no group origin")
            return
        try:
            from astrbot.api.event import MessageChain
            from astrbot.api.message_components import At, Plain

            mins = payload.get("minutes_before", 10)
            rname = payload.get("round_name", "第1组")
            title = payload.get("schedule_title", "") or ""
            for side, label, team in [
                ("blue_team", "蓝队", payload.get("blue_team", [])),
                ("red_team", "红队", payload.get("red_team", [])),
            ]:
                qq_list = [p.get("qq") for p in team if p.get("qq")]
                if not qq_list:
                    continue
                logger.warning(
                    f"[lolrace] remind: {title} {rname} {label} ({len(qq_list)} players, {mins}min), target={target_origin}"
                )
                comps = [
                    Plain(
                        f"🔔 {title} {rname} {label} 比赛将在 {mins} 分钟后开始，请准备\n"
                    )
                ]
                for q in qq_list:
                    comps.append(At(qq=q))
                    comps.append(Plain(" "))
                chain = MessageChain(comps)
                await self.context.send_message(target_origin, chain)
                await asyncio.sleep(5)
        except Exception as e:
            logger.warning(f"[lolrace] remind send failed: {e}")

    def _origin_for_group(self, group_id: str) -> str | None:
        if not self._group_origin:
            return None
        parts = self._group_origin.split(":GroupMessage:")
        if len(parts) == 2:
            return f"{parts[0]}:GroupMessage:{group_id}"
        return self._group_origin

    async def _send_auction_notify(self, payload: dict):
        """接收后端 auction_bot 频道的播报（action=notify），转发到指定群。"""
        if payload.get("action") != "notify":
            return
        group_id = str(payload.get("group_id") or "").strip()
        text = payload.get("text") or ""
        if not group_id or not text:
            return
        origin = self._origin_for_group(group_id)
        if not origin and self._group_origin:
            parts = self._group_origin.split(":")
            if len(parts) >= 3:
                origin = f"{parts[0]}:GroupMessage:{group_id}"
        if not origin:
            logger.warning(f"[lolrace] auction notify: no origin for group {group_id}")
            return
        try:
            from astrbot.api.event import MessageChain
            from astrbot.api.message_components import Plain

            await self.context.send_message(origin, MessageChain([Plain(text)]))
        except Exception as e:
            logger.warning(f"[lolrace] auction notify send failed: {e}")

    async def _send_bulletin(self, payload: dict):
        group_id = str(payload.get("group_id") or "").strip()
        target_origin = self._origin_for_group(group_id) or self._group_origin
        if not target_origin:
            logger.warning("[lolrace] bulletin: no group origin")
            return
        try:
            from astrbot.api.event import MessageChain
            from astrbot.api.message_components import At, Plain

            title = payload.get("title", "今日播报")
            content = payload.get("content", "")
            # Send text body first (guaranteed delivery)
            chain = MessageChain([Plain(f"📰 {title}\n\n{content}")])
            await self.context.send_message(target_origin, chain)
            # Try @mentions separately (may fail if UID unknown)
            at_qqs = payload.get("at_qqs", [])
            logger.info(f"[lolrace] bulletin at_qqs={at_qqs}")
            if at_qqs:
                await asyncio.sleep(2)
                comps = [Plain("相关选手：")]
                for qq in at_qqs:
                    comps.append(At(qq=str(qq)))
                    comps.append(Plain(" "))
                try:
                    await self.context.send_message(target_origin, MessageChain(comps))
                except Exception as e:
                    logger.warning(f"[lolrace] bulletin @mention failed: {e}")
        except Exception as e:
            logger.warning(f"[lolrace] bulletin send failed: {e}")

    @staticmethod
    def _sched_line(prefix: str, s: dict) -> str:
        """场次展示行：标题 + 时间 + 备注。"""
        line = f"「{prefix} {s.get('title')}」{s.get('start_time') or ''}"
        if s.get("description"):
            line += f" ｜ {s['description']}"
        return line

    def _get_args(self, event: AstrMessageEvent) -> list[str]:
        text = self._get_text(event)
        parts = text.split(maxsplit=1)
        return parts[1].split() if len(parts) > 1 else []

    def _get_group_id(self, event: AstrMessageEvent) -> str:
        candidates = [
            getattr(event, "group_id", None),
            getattr(getattr(event, "message_obj", None), "group_id", None),
        ]
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        if isinstance(raw, dict):
            candidates.append(raw.get("group_id"))
        for value in candidates:
            if value:
                return str(value)
        origin = str(getattr(event, "unified_msg_origin", "") or "")
        for sep in (":", ":GroupMessage:", "group_"):
            if sep in origin:
                tail = origin.rsplit(sep, 1)[-1]
                digits = "".join(ch for ch in tail if ch.isdigit())
                if digits:
                    return digits
        return ""

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def cache_group_message(self, event: AstrMessageEvent):
        try:
            text = self._get_text(event)
            if not text.strip():
                return
            self._record_origin(event)
            group_id = self._get_group_id(event)
            origin = str(getattr(event, "unified_msg_origin", "") or "")
            result = await self._api(
                "POST",
                "/bot/messages",
                json={
                    "group_id": group_id,
                    "group_origin": origin,
                    "user_id": event.get_sender_id(),
                    "nickname": event.get_sender_name(),
                    "content": text,
                },
            )
            if result and result.get("reason") == "group_mismatch":
                logger.warning(
                    f"[lolrace] bot message ignored by group mismatch, group_id={group_id}, origin={origin}"
                )
            elif result and result.get("web_login"):
                from astrbot.api.event import MessageChain
                from astrbot.api.message_components import Plain

                nick = (event.get_sender_name() or "").strip()
                await self.context.send_message(
                    origin,
                    MessageChain([Plain(f"✅ 网页登录成功：{nick}" if nick else "✅ 网页登录成功")]),
                )
            elif result and result.get("ok"):
                logger.debug(f"[lolrace] bot message cached, id={result.get('id')}")
        except Exception as e:
            logger.warning(f"[lolrace] cache_group_message error: {e}")

    @filter.command("帮助", priority=10)
    async def help_cmd(self, event: AstrMessageEvent):
        """这是一个帮助指令"""
        import os

        img_dir = os.path.join(os.path.dirname(__file__), "imgs")
        if os.path.isdir(img_dir):
            for f in os.listdir(img_dir):
                if f.startswith("帮助"):
                    yield event.image_result(os.path.join(img_dir, f))
                    return
        yield event.plain_result("❌ 帮助图片未找到")

    @filter.command("注册", priority=10)
    async def signup(self, event: AstrMessageEvent):
        """这是一个注册指令"""
        self._record_origin(event)
        logger.warning(f"[lolrace] signup messages: {event.get_messages()}")
        qq = event.get_sender_id()
        existing = await self._api("GET", f"/players/qq/{qq}")
        if existing and "id" in existing:
            yield event.plain_result(
                f"✅ 您已注册！\n当前信息：{self._name(existing)} | {existing['rank']} | {existing['primary_position']}/{existing['secondary_position']}\n如需修改请访问：{self.frontend_url}/#/show"
            )
            return
        nick = event.get_sender_name()
        result = await self._api(
            "POST", "/register/token", json={"qq": qq, "nick": nick}
        )
        if result and "token" in result:
            url = f"{self.frontend_url}/#/register?token={quote(str(result['token']))}"
            yield event.plain_result(
                f"🖊️ 请点击下方链接完成注册（1小时内有效）：\n{url}\n此链接仅限 {nick} (QQ: {qq}) 使用"
            )
        else:
            yield event.plain_result(self._api_err())

    @filter.command("登录", priority=10)
    async def auth_login(self, event: AstrMessageEvent):
        """这是一个身份认证指令"""
        self._record_origin(event)
        qq = event.get_sender_id()
        nick = event.get_sender_name()
        existing = await self._api("GET", f"/players/qq/{qq}")
        if existing is None:
            yield event.plain_result(self._api_err())
            return
        if "detail" in existing:
            yield event.plain_result("❌ 你还没有注册，请先发送：/注册")
            return
        yield event.plain_result(
            f"🔐 网页登录方式（30天免登录）：\n"
            f"1️⃣ 打开 {self.frontend_url}/#/show\n"
            f"2️⃣ 点击右上角「网页登录」，输入 QQ: {qq} 获取口令\n"
            f"3️⃣ 在本群发送「网页登录 口令」即可完成（口令=网页上获取的6位数字，如：网页登录 123456）\n"
            f"此方式仅限 {nick} 本人使用，无需私聊"
        )

    @filter.command("签到", priority=10)
    async def checkin(self, event: AstrMessageEvent):
        """这是一个签到指令"""
        self._record_origin(event)
        qq = event.get_sender_id()
        text = event.get_message_str().strip()
        title = text.replace("签到", "", 1).strip() or None

        scheds = await self._api("GET", "/schedules/")
        sched_map = {}
        auto_sid = None
        if scheds and isinstance(scheds, list):
            active = [s for s in scheds if s.get("is_active")]
            sched_map = {
                s["title"]: s.get("start_time") for s in scheds if s.get("title")
            }
            remark_map = {
                s["title"]: s.get("description") for s in scheds if s.get("title")
            }

        if not title:
            if scheds and isinstance(scheds, list):
                if len(active) > 1:
                    names = [self._sched_line("签到", s) for s in active if s.get("title")]
                    yield event.plain_result("❌ 请指定场次：\n" + "\n".join(names))
                    return
                elif len(active) == 1:
                    title = active[0].get("title")
                    auto_sid = active[0].get("id")
                else:
                    yield event.plain_result("❌ 今天没有可签到的场次")
                    return
            else:
                yield event.plain_result(self._api_err())
                return
        else:
            # 用户指定了title，尝试匹配活跃赛程的id
            for s in active if scheds and isinstance(scheds, list) else []:
                if s.get("title") == title:
                    auto_sid = s.get("id")
                    break

        if auto_sid:
            schedule_qs = f"?schedule_id={auto_sid}"
        elif title:
            schedule_qs = f"?schedule_title={quote(title)}"
        else:
            schedule_qs = ""
        st = sched_map.get(title, "") if title else ""

        status = await self._api("GET", f"/registrations/today/status{schedule_qs}")
        if status and status.get("is_closed"):
            yield event.plain_result("❌ 签到已截止")
            return
        player = await self._api("GET", f"/players/qq/{qq}")
        if player is None:
            yield event.plain_result(self._api_err())
            return
        if "detail" in player:
            yield event.plain_result("❌ 你还没有注册，请先发送：/注册")
            return
        if player.get("banned_until"):
            yield event.plain_result(
                f"❌ 你已被禁赛至 {player['banned_until']}，无法签到"
            )
            return
        result = await self._api(
            "POST", f"/registrations/signup{schedule_qs}", json={"qq": qq}
        )
        if result is None:
            yield event.plain_result(self._api_err())
        elif "detail" in result:
            msg = result["detail"]
            if "今天已经报名" in msg:
                msg = "你今天已经签过到了"
            yield event.plain_result(f"❌ {msg}")
        else:
            label = f" ({title}){' ' + st if st else ''}" if title else ""
            if title and remark_map.get(title):
                label += f" ｜ {remark_map[title]}"
            yield event.plain_result(f"✅ 签到成功{label}！{self._name(player)}")

    @filter.command("取消签到", priority=10)
    async def cancel_checkin(self, event: AstrMessageEvent):
        """这是一个取消签到指令"""
        self._record_origin(event)
        qq = event.get_sender_id()
        text = event.get_message_str().strip()
        title = text.replace("取消签到", "", 1).strip() or None

        scheds = await self._api("GET", "/schedules/")
        sched_map = {}
        auto_sid = None
        if scheds and isinstance(scheds, list):
            active = [s for s in scheds if s.get("is_active")]
            sched_map = {
                s["title"]: s.get("start_time") for s in scheds if s.get("title")
            }
            remark_map = {
                s["title"]: s.get("description") for s in scheds if s.get("title")
            }

        if not title:
            if scheds and isinstance(scheds, list):
                if len(active) > 1:
                    names = [self._sched_line("取消签到", s) for s in active if s.get("title")]
                    yield event.plain_result("❌ 请指定场次：\n" + "\n".join(names))
                    return
                elif len(active) == 1:
                    title = active[0].get("title")
                    auto_sid = active[0].get("id")
        else:
            # 用户指定了title，尝试匹配活跃赛程的id
            for s in active if scheds and isinstance(scheds, list) else []:
                if s.get("title") == title:
                    auto_sid = s.get("id")
                    break

        if auto_sid:
            schedule_qs = f"?schedule_id={auto_sid}"
        elif title:
            schedule_qs = f"?schedule_title={quote(title)}"
        else:
            schedule_qs = ""
        st = sched_map.get(title, "") if title else ""
        result = await self._api(
            "POST", f"/registrations/cancel{schedule_qs}", json={"qq": qq}
        )
        if result is None:
            yield event.plain_result(self._api_err())
        elif "detail" in result:
            yield event.plain_result(f"❌ {result['detail']}")
        else:
            label = f" ({title}){' ' + st if st else ''}" if title else ""
            if title and remark_map.get(title):
                label += f" ｜ {remark_map[title]}"
            yield event.plain_result(f"✅ 已取消签到{label}")

    @filter.command("我的战绩", priority=10)
    async def my_stats(self, event: AstrMessageEvent):
        """这是一个查询个人战绩指令"""
        qq = event.get_sender_id()
        p = await self._api("GET", f"/players/qq/{qq}")
        if p is None:
            yield event.plain_result(self._api_err())
            return
        if "detail" in p:
            yield event.plain_result("❌ 你还没有注册，请先发送：/注册")
            return
        rate = (
            f"{p['wins'] / p['total_matches'] * 100:.0f}%"
            if p.get("total_matches")
            else "-"
        )
        yield event.plain_result(
            f"选手：{self._name(p)}\n段位：{p['rank']}\n位置：{p['primary_position']}/{p['secondary_position']}\n"
            f"战绩：{p.get('wins', 0)}胜/{p.get('total_matches', 0)}场 ({rate})\nMVP：{p.get('mvp_count', 0)}次\n"
            f"📊 也可在网页查看：{self.frontend_url}/#/show"
        )

    @filter.command("查询", priority=10)
    async def query(self, event: AstrMessageEvent):
        """这是一个查询选手信息指令"""
        mvp_qq = self._get_mvp_mention(event)
        if mvp_qq:
            p = await self._api("GET", f"/players/qq/{mvp_qq}")
            if p is None:
                yield event.plain_result(self._api_err())
                return
            if "detail" in p:
                yield event.plain_result("❌ 该群友未注册")
                return
        else:
            args = self._get_args(event)
            if not args:
                yield event.plain_result("用法：查询 <电1游戏ID> 或 <QQ昵称> 或 @群友")
                return
            keyword = args[0]
            players = await self._api("GET", "/players/")
            if not players or not isinstance(players, list):
                yield event.plain_result(
                    self._api_err() if players is None else "❌ 查询失败"
                )
                return
            matches = [
                p
                for p in players
                if keyword.lower() in (p.get("group_id") or "").lower()
                or keyword.lower() in (p.get("game_name") or "").lower()
            ]
            if not matches:
                yield event.plain_result(f"未找到「{keyword}」")
                return
            p = matches[0]
        rate = (
            f"{p['wins'] / p['total_matches'] * 100:.0f}%"
            if p.get("total_matches")
            else "-"
        )
        yield event.plain_result(
            f"选手：{self._name(p)}\n电1游戏ID：{p['game_name']}\n段位：{p['rank']}\n"
            f"位置：{p['primary_position']}/{p['secondary_position']}\n"
            f"战绩：{p.get('wins', 0)}胜/{p.get('total_matches', 0)}场 ({rate})\nMVP：{p.get('mvp_count', 0)}次\n"
            f"📊 也可在网页查看：{self.frontend_url}/#/show"
        )

    @filter.command("修改信息", priority=10)
    async def update_info(self, event: AstrMessageEvent):
        """这是一个修改个人信息指令"""
        yield event.plain_result(
            "修改信息已迁移到网页端\n"
            f"请访问：{self.frontend_url}/#/show\n"
            "双击选手卡片即可修改"
        )

    def _get_mvp_mention(self, event: AstrMessageEvent) -> str | None:
        comps = event.get_messages()
        found_bot = False
        for comp in comps:
            if comp.type == "At":
                if not found_bot:
                    found_bot = True
                    continue
                return str(comp.qq)
        return None

    @filter.command("登记战绩", priority=10)
    async def record_match(self, event: AstrMessageEvent):
        """这是一个登记比赛结果指令"""
        yield event.plain_result("❌ 登记战绩已迁移到网页端，请联系管理员提供战绩图片")

    @filter.command("同步名称", priority=10)
    async def sync_name(self, event: AstrMessageEvent):
        """这是一个同步当前群昵称指令"""
        qq = event.get_sender_id()
        name = event.get_sender_name()
        result = await self._api(
            "POST", "/players/sync-name", json={"qq": qq, "name": name}
        )
        if result is None:
            yield event.plain_result(self._api_err())
        elif "detail" in result:
            yield event.plain_result(f"❌ {result['detail']}")
        else:
            yield event.plain_result("✅ 名称已同步")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("切换签到", priority=10)
    async def toggle_signup(self, event: AstrMessageEvent):
        """这是一个切换签到状态指令（管理员专用）"""
        self._record_origin(event)
        text = event.get_message_str().strip()
        title = text.replace("切换签到", "", 1).strip() or None
        schedule_qs = f"?schedule_title={quote(title)}" if title else ""
        result = await self._api("POST", f"/registrations/toggle{schedule_qs}")
        if result:
            rtitle = result.get("title") or title
            rtime = result.get("start_time", "")
            label = f" ({rtitle}){' ' + rtime if rtime else ''}" if rtitle else ""
            if result.get("description"):
                label += f" ｜ {result['description']}"
            yield event.plain_result(f"✅ 已{result['message']}{label}")
        else:
            yield event.plain_result(self._api_err())

    # ================= 比赛侧 =================

    @filter.command("比赛报名", priority=10)
    async def tournament_signup(self, event: AstrMessageEvent):
        """报名参加比赛"""
        self._record_origin(event)
        qq = event.get_sender_id()
        text = event.get_message_str().strip()
        name = text.replace("比赛报名", "", 1).strip() or None
        tournaments = await self._api("GET", "/tournaments/")
        if tournaments is None:
            yield event.plain_result(self._api_err())
            return
        if not isinstance(tournaments, list) or not tournaments:
            yield event.plain_result("❌ 当前没有赛事")
            return
        target = None
        if name:
            target = next((t for t in tournaments if t.get("name") == name), None)
            if not target:
                yield event.plain_result(f"❌ 未找到赛事「{name}」")
                return
        else:
            open_ones = [t for t in tournaments if t.get("signup_open")]
            if not open_ones:
                yield event.plain_result("❌ 当前没有开放报名的赛事")
                return
            if len(open_ones) > 1:
                lines = [f"「比赛报名 {t['name']}」" for t in open_ones]
                yield event.plain_result("❌ 有多个赛事开放报名，请指定：\n" + "\n".join(lines))
                return
            target = open_ones[0]
        result = await self._api(
            "POST", f"/tournaments/{target['id']}/signup", json={"qq": qq}
        )
        if result is None:
            yield event.plain_result(self._api_err())
        elif "detail" in result:
            yield event.plain_result(f"❌ {result['detail']}")
        else:
            # 群里只发普通链接；网页登录走群口令（token 不出现在群里，也避免私聊触发风控）
            link = f"{self.frontend_url}/#/tournament/{target['id']}"
            yield event.plain_result(
                self._tournament_signup_card(target, qq, await self._api("GET", f"/tournaments/{target['id']}"))
                + f"\n🌐 详情与对阵图：{link}\n"
                f"💡 首次使用：打开链接点击「网页登录」，在群里发出口令即可（30天免登录）"
            )

    @staticmethod
    def _tournament_signup_card(target: dict, qq: str, detail: dict | None) -> str:
        """报名成功回执卡片：赛事关键信息一览。"""
        status_map = {
            "signup": "报名中", "team_building": "建队中",
            "ongoing": "进行中", "finished": "已结束", "cancelled": "已取消",
        }
        lines = [f"✅ 报名成功！（选手：{qq}）", "", f"【{target.get('name', '赛事')}】"]

        meta = []
        status = status_map.get(target.get("status") or (detail or {}).get("status"), "")
        if status:
            meta.append(status)
        if detail and detail.get("bo"):
            meta.append(f"BO{detail['bo']} 单败淘汰")
        if target.get("start_date"):
            meta.append(f"{target['start_date']} 开赛")
        if meta:
            lines.append(" · ".join(meta))

        if detail and detail.get("description"):
            lines.append(f"📝 {str(detail['description'])[:80]}")

        signup_count = (target.get("signup_count") or 0) + 1
        team_count = len((detail or {}).get("teams") or []) or target.get("team_count") or 0
        lines.append(f"👥 本次报名后共 {signup_count} 人报名 · {team_count} 支战队")

        bracket = (detail or {}).get("bracket") or []
        if bracket:
            total = sum(len(r.get("matches", [])) for r in bracket)
            finished = sum(
                1 for r in bracket for m in r.get("matches", []) if m.get("status") == "finished"
            )
            if total:
                lines.append(f"⚔️ 对阵进度：{finished}/{total} 场已打完")
        return "\n".join(lines)

    @filter.command("取消报名", priority=10)
    async def tournament_cancel_signup(self, event: AstrMessageEvent):
        """取消比赛报名"""
        self._record_origin(event)
        qq = event.get_sender_id()
        text = event.get_message_str().strip()
        name = text.replace("取消报名", "", 1).strip() or None
        tournaments = await self._api("GET", "/tournaments/")
        if tournaments is None:
            yield event.plain_result(self._api_err())
            return
        target = None
        if name:
            target = next((t for t in tournaments if t.get("name") == name), None)
        elif isinstance(tournaments, list) and tournaments:
            target = tournaments[0]
        if not target:
            yield event.plain_result("❌ 未找到赛事")
            return
        result = await self._api(
            "POST", f"/tournaments/{target['id']}/signup/cancel", json={"qq": qq}
        )
        if result is None:
            yield event.plain_result(self._api_err())
        elif "detail" in result:
            yield event.plain_result(f"❌ {result['detail']}")
        else:
            yield event.plain_result(f"✅ 已取消「{target['name']}」的报名")

    @filter.command("赛程", priority=10)
    async def tournament_schedule(self, event: AstrMessageEvent):
        """查询赛事对阵与赛程"""
        tournaments = await self._api("GET", "/tournaments/")
        if tournaments is None:
            yield event.plain_result(self._api_err())
            return
        if not isinstance(tournaments, list) or not tournaments:
            yield event.plain_result("❌ 当前没有赛事")
            return
        active = [
            t for t in tournaments
            if t.get("status") in ("ongoing", "signup", "team_building")
        ]
        if not active:
            finished = [t for t in tournaments if t.get("status") == "finished"]
            if finished:
                yield event.plain_result(
                    "🏁 赛事已全部结束\n"
                    + "\n".join(f"🏆 {t['name']}" for t in finished[:3])
                    + f"\n🌐 {self.frontend_url}/#/tournament"
                )
            else:
                yield event.plain_result("❌ 当前没有进行中的赛事")
            return
        lines = []
        for t in active[:2]:
            lines.append(f"【{t['name']}】")
            detail = await self._api("GET", f"/tournaments/{t['id']}")
            if not detail:
                continue
            bracket = detail.get("bracket", [])
            upcoming = []
            for rd in bracket:
                for m in rd.get("matches", []):
                    if m.get("status") == "finished":
                        continue
                    t1 = (m.get("team1") or {}).get("name", "待定")
                    t2 = (m.get("team2") or {}).get("name", "待定")
                    upcoming.append(
                        f"  {rd['title']}：{t1} {m.get('score1', 0)}:{m.get('score2', 0)} {t2}（BO{m.get('bo', 3)}）"
                    )
            lines.extend(upcoming[:6] or ["  暂无待赛对阵"])
        lines.append(f"🌐 完整对阵图：{self.frontend_url}/#/tournament")
        yield event.plain_result("\n".join(lines))
