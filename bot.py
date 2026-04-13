"""
Discord Weekly Post Tracker Bot
Tracks whether members posted in specific channels each week.
"""

import discord
from discord import app_commands
from discord.ext import commands, tasks
import sqlite3
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
TOKEN = os.environ.get("DISCORD_TOKEN", "YOUR_BOT_TOKEN_HERE")
DB_PATH = "data/tracker.db"

# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────

def get_db():
    """Return a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tracked_channels (
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                added_by    TEXT,
                added_at    TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (guild_id, channel_id)
            );

            CREATE TABLE IF NOT EXISTS weekly_posts (
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                week_start  TEXT NOT NULL,   -- ISO date of Monday
                posted_at   TEXT NOT NULL,   -- first post timestamp that week
                PRIMARY KEY (guild_id, channel_id, user_id, week_start)
            );
        """)
    log.info("Database ready.")

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def week_start(dt: datetime) -> str:
    """Return ISO string of the Monday starting the week containing dt."""
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")

def is_tracked(guild_id: str, channel_id: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM tracked_channels WHERE guild_id=? AND channel_id=?",
            (guild_id, channel_id)
        ).fetchone()
    return row is not None

def record_post(guild_id: str, channel_id: str, user_id: str, posted_at: datetime):
    """Insert a weekly post record (ignored if already exists for that week)."""
    ws = week_start(posted_at)
    with get_db() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO weekly_posts
               (guild_id, channel_id, user_id, week_start, posted_at)
               VALUES (?, ?, ?, ?, ?)""",
            (guild_id, channel_id, user_id, ws, posted_at.isoformat())
        )

def get_tracked_channels(guild_id: str) -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT channel_id FROM tracked_channels WHERE guild_id=?",
            (guild_id,)
        ).fetchall()
    return [r["channel_id"] for r in rows]

def get_stats(guild_id: str, channel_id: str, weeks: int = 8):
    """
    Return stats for the last `weeks` weeks.
    Returns dict: user_id -> {"weeks_posted": int, "weeks": {week_start: bool}}
    """
    # Build list of the last N week-start dates
    today = datetime.now(timezone.utc)
    week_starts = []
    for i in range(weeks - 1, -1, -1):
        d = today - timedelta(weeks=i)
        week_starts.append(week_start(d))

    with get_db() as conn:
        rows = conn.execute(
            """SELECT user_id, week_start FROM weekly_posts
               WHERE guild_id=? AND channel_id=? AND week_start >= ?""",
            (guild_id, channel_id, week_starts[0])
        ).fetchall()

    # Organize: user_id -> set of weeks they posted
    user_weeks: dict[str, set] = defaultdict(set)
    for row in rows:
        user_weeks[row["user_id"]].add(row["week_start"])

    # Build result
    result = {}
    for user_id, posted_set in user_weeks.items():
        result[user_id] = {
            "weeks_posted": len(posted_set & set(week_starts)),
            "weeks": {ws: (ws in posted_set) for ws in week_starts}
        }
    return result, week_starts

def calculate_streaks(guild_id: str, channel_id: str):
    """Calculate current consecutive-week streaks for all users."""
    today = datetime.now(timezone.utc)
    ws_list = [week_start(today - timedelta(weeks=i)) for i in range(52)]  # up to 1 year
    ws_list.sort()  # oldest first

    with get_db() as conn:
        rows = conn.execute(
            "SELECT user_id, week_start FROM weekly_posts WHERE guild_id=? AND channel_id=?",
            (guild_id, channel_id)
        ).fetchall()

    user_weeks: dict[str, set] = defaultdict(set)
    for row in rows:
        user_weeks[row["user_id"]].add(row["week_start"])

    streaks = {}
    for user_id, posted in user_weeks.items():
        streak = 0
        # Walk backward from most recent week
        for ws in reversed(ws_list):
            if ws in posted:
                streak += 1
            else:
                break
        streaks[user_id] = streak

    return streaks

# ──────────────────────────────────────────────
# Bot setup
# ──────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# ──────────────────────────────────────────────
# Events
# ──────────────────────────────────────────────

@bot.event
async def on_ready():
    log.info(f"Logged in as {bot.user} ({bot.user.id})")
    init_db()
    try:
        synced = await bot.tree.sync()
        log.info(f"Synced {len(synced)} slash commands.")
    except Exception as e:
        log.error(f"Failed to sync commands: {e}")

@bot.event
async def on_message(message: discord.Message):
    """Record a post whenever someone messages in a tracked channel."""
    if message.author.bot:
        return
    if message.guild is None:
        return

    guild_id = str(message.guild.id)
    channel_id = str(message.channel.id)

    if is_tracked(guild_id, channel_id):
        record_post(
            guild_id,
            channel_id,
            str(message.author.id),
            message.created_at.replace(tzinfo=timezone.utc)
        )

    await bot.process_commands(message)

# ──────────────────────────────────────────────
# Slash Commands
# ──────────────────────────────────────────────

@bot.tree.command(name="backfill", description="Backfill history for a tracked channel (admin only).")
@app_commands.describe(
    channel="The channel to backfill",
    limit="How many messages to scan (default 1000, max 10000)"
)
@app_commands.checks.has_permissions(manage_channels=True)
async def backfill(interaction: discord.Interaction, channel: discord.TextChannel, limit: int = 1000):
    await interaction.response.defer(ephemeral=True)

    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)

    if not is_tracked(guild_id, channel_id):
        await interaction.followup.send(f"{channel.mention} is not tracked.", ephemeral=True)
        return

    limit = max(1, min(limit, 10000))
    count = 0

    async for message in channel.history(limit=limit, oldest_first=True):
        if not message.author.bot:
            record_post(
                guild_id,
                channel_id,
                str(message.author.id),
                message.created_at.replace(tzinfo=timezone.utc)
            )
            count += 1

    await interaction.followup.send(
        f"✅ Backfilled {count} messages from {channel.mention}.", ephemeral=True
    )

@bot.tree.command(name="debug", description="Show raw DB entries for a channel.")
@app_commands.checks.has_permissions(manage_channels=True)
async def debug(interaction: discord.Interaction, channel: discord.TextChannel):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT user_id, week_start, posted_at FROM weekly_posts WHERE channel_id=? ORDER BY week_start DESC LIMIT 20",
            (str(channel.id),)
        ).fetchall()
    if not rows:
        await interaction.response.send_message("No rows found for that channel.", ephemeral=True)
        return
    lines = [f"`{r['user_id']} | {r['week_start']} | {r['posted_at']}`" for r in rows]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@bot.tree.command(name="track-channel", description="Start tracking weekly posts in a channel (admin only).")
@app_commands.describe(channel="The channel to track")
@app_commands.checks.has_permissions(manage_channels=True)
async def track_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)

    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO tracked_channels (guild_id, channel_id, added_by) VALUES (?, ?, ?)",
            (guild_id, channel_id, str(interaction.user.id))
        )

    await interaction.response.send_message(
        f"✅ Now tracking weekly posts in {channel.mention}.", ephemeral=True
    )
    log.info(f"Tracking {channel.name} in guild {interaction.guild.name}")


@bot.tree.command(name="untrack", description="Stop tracking a channel (admin only).")
@app_commands.describe(channel="The channel to stop tracking")
@app_commands.checks.has_permissions(manage_channels=True)
async def untrack(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)

    with get_db() as conn:
        conn.execute(
            "DELETE FROM tracked_channels WHERE guild_id=? AND channel_id=?",
            (guild_id, channel_id)
        )

    await interaction.response.send_message(
        f"🛑 Stopped tracking {channel.mention}. Historical data is preserved.", ephemeral=True
    )


@bot.tree.command(name="list-tracked", description="List all currently tracked channels.")
async def list_tracked(interaction: discord.Interaction):
    channel_ids = get_tracked_channels(str(interaction.guild_id))

    if not channel_ids:
        await interaction.response.send_message(
            "No channels are being tracked yet. Use `/track-channel` to add one.",
            ephemeral=True
        )
        return

    mentions = []
    for cid in channel_ids:
        ch = interaction.guild.get_channel(int(cid))
        mentions.append(ch.mention if ch else f"<#{cid}> (deleted)")

    await interaction.response.send_message(
        "**Tracked channels:**\n" + "\n".join(mentions), ephemeral=True
    )


@bot.tree.command(name="stats", description="Show weekly posting stats for a tracked channel.")
@app_commands.describe(
    channel="The tracked channel to view",
    weeks="Number of weeks to look back (default: 8)"
)
async def stats(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    weeks: int = 8
):
    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)
    weeks = max(1, min(weeks, 26))  # clamp 1-26
    if not is_tracked(guild_id, channel_id):
        await interaction.followup.send(
            f"{channel.mention} is not being tracked. Use `/track-channel` first.",
            ephemeral=True
        )
        return
    
    user_data, week_starts = get_stats(guild_id, channel_id, weeks)
    log.info(f"Querying from week_start >= {week_starts[0]}, channel={channel_id}, guild={guild_id}")
    if not user_data:
        await interaction.followup.send(
            f"No posts recorded in {channel.mention} yet.",
            ephemeral=True
        )
        return

    # Sort users by weeks posted (desc)
    sorted_users = sorted(user_data.items(), key=lambda x: x[1]["weeks_posted"], reverse=True)
    log.info(f"sorted_users count: {len(sorted_users)}")

    # Build header row (abbreviated dates)
    date_headers = "  ".join(ws[5:] for ws in week_starts)  # MM-DD

    lines = [f"**{channel.mention} — Last {weeks} weeks**"]
    lines.append(f"```")
    lines.append(f"{'User':<22} {date_headers}")
    lines.append("─" * (22 + len(date_headers) + 2))

    for user_id, data in sorted_users[:25]:
        try:
            member = interaction.guild.get_member(int(user_id))
            if not member:
                try:
                    member = await interaction.guild.fetch_member(int(user_id))
                except discord.NotFound:
                    pass
            name = (member.display_name if member else f"User {user_id[:6]}")[:20]
            week_row = "      ".join("✓" if data["weeks"][ws] else "✗" for ws in week_starts)
            lines.append(f"{name:<22} {week_row}   ({data['weeks_posted']}/{weeks})")
        except Exception as e:
            log.error(f"Error processing user {user_id}: {e}")

    lines.append("```")

    lines.append("✓ = posted at least once that week  ✗ = no post")
    await interaction.followup.send("\n".join(lines))

@bot.tree.command(name="mystats", description="Show your own weekly posting stats.")
@app_commands.describe(
    channel="The tracked channel to view",
    weeks="Number of weeks to look back (default: 8)"
)
async def mystats(interaction: discord.Interaction, channel: discord.TextChannel, weeks: int = 8):
    await interaction.response.defer(ephemeral=True)

    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)
    user_id = str(interaction.user.id)
    weeks = max(1, min(weeks, 520))

    if not is_tracked(guild_id, channel_id):
        await interaction.followup.send(f"{channel.mention} is not tracked.", ephemeral=True)
        return

    user_data, week_starts = get_stats(guild_id, channel_id, weeks)

    if user_id not in user_data:
        await interaction.followup.send(
            f"No posts recorded for you in {channel.mention}.", ephemeral=True
        )
        return

    data = user_data[user_id]
    streaks = calculate_streaks(guild_id, channel_id)
    streak = streaks.get(user_id, 0)
    streak_str = f"  🔥 {streak} week streak" if streak >= 2 else ""

    date_headers = "  ".join(ws[5:] for ws in week_starts)
    week_row = "      ".join("✓" if data["weeks"][ws] else "✗" for ws in week_starts)
    divider = "─" * (len(date_headers) + 2)

    await interaction.followup.send(
        f"**Your stats in #{channel.name} (last {weeks} weeks):**\n"
        f"```{date_headers}\n{divider}\n{week_row}```"
        f"{data['weeks_posted']}/{weeks} weeks posted{streak_str}",
        ephemeral=True
    )

@bot.tree.command(name="leaderboard", description="Show a posting consistency leaderboard.")
@app_commands.describe(
    channel="The tracked channel to rank",
    month="Month to filter by (1-12, default: all time)",
    year="Year to filter by (e.g. 2026, default: current year)"
)
async def leaderboard(interaction: discord.Interaction, channel: discord.TextChannel, month: int = None, year: int = None):
    await interaction.response.defer()

    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)

    if not is_tracked(guild_id, channel_id):
        await interaction.followup.send(f"{channel.mention} is not tracked.", ephemeral=True)
        return

    if year is None:
        year = datetime.now(timezone.utc).year

    # Build list of week_starts that fall within the requested month/year
    with get_db() as conn:
        if month is not None:
            # Filter to weeks that started in the given month/year
            month_str = f"{year}-{month:02d}"
            rows = conn.execute(
                """SELECT user_id, COUNT(DISTINCT week_start) as weeks_posted
                   FROM weekly_posts
                   WHERE guild_id=? AND channel_id=? AND week_start LIKE ?
                   GROUP BY user_id""",
                (guild_id, channel_id, f"{month_str}%")
            ).fetchall()
            title_period = f"{year}-{month:02d}"
        else:
            rows = conn.execute(
                """SELECT user_id, COUNT(DISTINCT week_start) as weeks_posted
                   FROM weekly_posts
                   WHERE guild_id=? AND channel_id=?
                   GROUP BY user_id""",
                (guild_id, channel_id)
            ).fetchall()
            title_period = "all time"

    if not rows:
        await interaction.followup.send("No data for that period.", ephemeral=True)
        return

    streaks = calculate_streaks(guild_id, channel_id)
    ranked = sorted(rows, key=lambda r: (r["weeks_posted"], streaks.get(r["user_id"], 0)), reverse=True)

    medals = ["🥇", "🥈", "🥉"]
    lines = []

    for i, row in enumerate(ranked[:10]):
        user_id = row["user_id"]
        member = interaction.guild.get_member(int(user_id))
        if not member:
            try:
                member = await interaction.guild.fetch_member(int(user_id))
            except discord.NotFound:
                pass
        name = member.display_name if member else f"User {user_id[:6]}"
        pos = medals[i] if i < 3 else f"`{i+1}.`"
        streak = streaks.get(user_id, 0)
        streak_str = f"🔥 {streak}w streak" if streak >= 2 else ""
        lines.append(f"{pos} **{name}** — {row['weeks_posted']} weeks {streak_str}")

    await interaction.followup.send(
        f"🏆 **Leaderboard — #{channel.name}** ({title_period})\n" + "\n".join(lines)
    )


@bot.tree.command(name="report", description="Who posted (or missed) this week in a channel?")
@app_commands.describe(channel="The tracked channel to report on")
async def report(interaction: discord.Interaction, channel: discord.TextChannel):
    await interaction.response.defer()


    guild_id = str(interaction.guild_id)
    channel_id = str(channel.id)

    if not is_tracked(guild_id, channel_id):
        await interaction.followup.send(
            f"{channel.mention} is not tracked.", ephemeral=True
        )
        return

    user_data, week_starts = get_stats(guild_id, channel_id, 1)
    current_week = week_starts[0]

    posted = [uid for uid, d in user_data.items() if d["weeks"][current_week]]
    # Note: "missed" only makes sense for users who ever participated
    missed = [uid for uid, d in user_data.items() if not d["weeks"][current_week]]

    def mention_list(ids):
        parts = []
        for uid in ids[:20]:
            m = interaction.guild.get_member(int(uid))
            parts.append(m.mention if m else f"<@{uid}>")
        return "\n".join(parts) if parts else "*nobody yet*"

    await interaction.followup.send(
        f"📅 **Weekly Report — #{channel.name}** (week of {current_week})\n\n"
        f"✅ **Posted ({len(posted)}):**\n{mention_list(posted)}\n\n"
        f"❌ **Missed ({len(missed)}):**\n{mention_list(missed)}"
    )


# Error handling
@track_channel.error
@untrack.error
async def admin_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You need **Manage Channels** permission to use this command.",
            ephemeral=True
        )


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    if TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("⚠️  Set your bot token in the DISCORD_TOKEN environment variable.")
        print("    export DISCORD_TOKEN=your_token_here")
        exit(1)
    bot.run(TOKEN)
    