import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from PIL import Image
from datetime import datetime
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import madflixbotz
from config import Config
import random
from helper.ffmpeg import fix_thumb, take_screen_shot
import asyncio
import os
import time
import re

renaming_operations = {}

# EPISODE PATTERNS
# Pattern 1: S01E02 or S01EP02
pattern1 = re.compile(r'S(\d+)(?:E|EP)(\d+)')
# Pattern 2: S01 E02 or S01 EP02 or S01 - E01 or S01 - EP02
pattern2 = re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)')
# Pattern 3: Episode Number After "E" or "EP"
pattern3 = re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)')
# Pattern 3_2: Episode number after hyphen
pattern3_2 = re.compile(r'(?:\s*-\s*(\d+)\s*)')
# Pattern 4: S2 09 example
pattern4 = re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE)
# Pattern X: Standalone Episode Number
patternX = re.compile(r'(\d+)')

# QUALITY PATTERNS
# Combined pattern to find 3-4 digits before 'p', '4k', '2k', 'HdRip', '4kX264', '4kx265'
quality_patterns = [
    re.compile(r'\b(?:.*?(\d{3,4}[^\dp]*p).*?|.*?(\d{3,4}p))\b', re.IGNORECASE),
    re.compile(r'[([<{]?\s*4k\s*[)\]>}]?', re.IGNORECASE),
    re.compile(r'[([<{]?\s*2k\s*[)\]>}]?', re.IGNORECASE),
    re.compile(r'[([<{]?\s*HdRip\s*[)\]>}]?|\bHdRip\b', re.IGNORECASE),
    re.compile(r'[([<{]?\s*4kX264\s*[)\]>}]?', re.IGNORECASE),
    re.compile(r'[([<{]?\s*4kx265\s*[)\]>}]?', re.IGNORECASE),
]

def extract_quality(filename):
    for pattern in quality_patterns:
        match = re.search(pattern, filename)
        if match:
            quality = match.group(1) or match.group(0).strip('()[]{}<>' + ' \t\n\r')  # Extract quality
            print(f"Matched Quality Pattern: {pattern.pattern}")
            print(f"Quality: {quality}")
            return quality
    print("Quality: Unknown")
    return "Unknown"

def extract_episode_number(filename):    
    episode_patterns = [pattern1, pattern2, pattern3, pattern3_2, pattern4, patternX]
    
    for pattern in episode_patterns:
        match = re.search(pattern, filename)
        if match:
            episode_number = match.group(2) if pattern in [pattern1, pattern2, pattern4] else match.group(1)
            print(f"Matched Episode Pattern: {pattern.pattern}")
            print(f"Episode Number: {episode_number}")
            return episode_number
    
    print("Episode Number: None")
    return None

# Example Usage
filename = "Naruto Shippuden S01 - EP07 - 1080p [Dual Audio] @Madflix_Bots.mkv"
episode_number = extract_episode_number(filename)
print(f"Extracted Episode Number: {episode_number}")

# Thread pool executor
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Download function
def download_media(client, message, file_path, download_msg):
    try:
        path = client.download_media(
            message=message,
            file_name=file_path,
            progress=progress_for_pyrogram,
            progress_args=("Download Started....", download_msg, time.time())
        )
        return path
    except Exception as e:
        return str(e)

# Upload function
def upload_media(client, message, metadata_path, ph_path, caption, media_type, duration, upload_msg):
    try:
        if media_type == "document":
            client.send_document(
                message.chat.id,
                document=metadata_path,
                thumb=ph_path,
                caption=caption,
                progress=progress_for_pyrogram,
                progress_args=("Upload Started.....", upload_msg, time.time())
            )
        elif media_type == "video":
            client.send_video(
                message.chat.id,
                video=metadata_path,
                caption=caption,
                thumb=ph_path,
                duration=duration,
                progress=progress_for_pyrogram,
                progress_args=("Upload Started.....", upload_msg, time.time())
            )
        elif media_type == "audio":
            client.send_audio(
                message.chat.id,
                audio=metadata_path,
                caption=caption,
                thumb=ph_path,
                duration=duration,
                progress=progress_for_pyrogram,
                progress_args=("Upload Started.....", upload_msg, time.time())
            )
    except Exception as e:
        return str(e)

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    user_id = message.from_user.id
    firstname = message.from_user.first_name
    format_template = await madflixbotz.get_format_template(user_id)
    media_preference = await madflixbotz.get_media_preference(user_id)

    if not format_template:
        return await message.reply_text("Please Set An Auto Rename Format First Using /autorename")

    # Extract information from the incoming file name
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        media_type = media_preference or "document"
    elif message.video:
        file_id = message.video.file_id
        file_name = f"{message.video.file_name}.mkv"
        media_type = media_preference or "video"
    elif message.audio:
        file_id = message.audio.file_id
        file_name = f"{message.audio.file_name}.mp3"
        media_type = media_preference or "audio"
    else:
        return await message.reply_text("Unsupported File Type")

    print(f"Original File Name: {file_name}")

    # Check if the file is being renamed
    if file_id in renaming_operations:
        elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
        if elapsed_time < 10:
            print("File is being ignored as it is currently being renamed or was renamed recently.")
            return

    renaming_operations[file_id] = datetime.now()

    episode_number = extract_episode_number(file_name)
    print(f"Extracted Episode Number: {episode_number}")

    if episode_number:
        placeholders = ["episode", "Episode", "EPISODE", "{episode}"]
        for placeholder in placeholders:
            format_template = format_template.replace(placeholder, str(episode_number), 1)
        
        quality_placeholders = ["quality", "Quality", "QUALITY", "{quality}"]
        for quality_placeholder in quality_placeholders:
            if quality_placeholder in format_template:
                extracted_qualities = extract_quality(file_name)
                if extracted_qualities == "Unknown":
                    await message.reply_text("I Was Not Able To Extract The Quality Properly. Renaming As 'Unknown'...")
                    del renaming_operations[file_id]
                    return
                
                format_template = format_template.replace(quality_placeholder, "".join(extracted_qualities))

        _, file_extension = os.path.splitext(file_name)
        new_file_name = f"{format_template}{file_extension}"
        file_path = f"downloads/{new_file_name}"
        file = message

        download_msg = await message.reply_text(text="Trying To Download.....")
        
        # Use the thread pool for downloading
        download_future = executor.submit(download_media, client, message, file_path, download_msg)
        path = download_future.result()
        
        if isinstance(path, str) and "Error" in path:
            del renaming_operations[file_id]
            return await download_msg.edit(path)

        if not os.path.isdir("Metadata"):
            os.mkdir("Metadata")

        _bool_metadata = await madflixbotz.get_metadata(message.chat.id)
        if _bool_metadata:
            metadata_path = f"Metadata/{new_file_name}"
            metadata = await madflixbotz.get_metadata_code(message.chat.id)
            if metadata:
                await download_msg.edit("I Found Your Metadata\n\n__**Adding Metadata To File....**")
                cmd = f"""ffmpeg -i "{path}" -map 0 -c:s copy -c:a copy -c:v copy -metadata title="{metadata}" -metadata author="{metadata}" -metadata:s:s title="{metadata}" -metadata:s:a title="{metadata}" -metadata:s:v title="{metadata}" "{metadata_path}" """
                
                process = await asyncio.create_subprocess_shell(
                    cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                er = stderr.decode()
                if er:
                    return await download_msg.edit(str(er) + "\n\n**Error**")

            await download_msg.edit("**Metadata added to the file successfully ✅**\n\n⚠️ __**Trying To Uploading....**")
        else:
            await download_msg.edit("⚠️  __**Please wait...**__\n\n\n**Trying To Uploading....**")

        duration = 0
        try:
            parser = createParser(file_path)
            metadata = extractMetadata(parser)
            if metadata.has("duration"):
                duration = metadata.get('duration').seconds
            parser.close()
        except:
            pass

        upload_msg = await download_msg.edit("Trying To Uploading.....")
        ph_path = None
        media = getattr(file, file.media.value)
        c_caption = await madflixbotz.get_caption(message.chat.id)
        c_thumb = await madflixbotz.get_thumbnail(message.chat.id)

        if c_caption:
            try:
                caption = c_caption.format(filename=new_file_name, filesize=humanbytes(media.file_size), duration=convert(duration))
            except Exception as e:
                return await download_msg.edit(text=f"Your Caption Error Except Keyword Argument ●> ({e})")
        else:
            caption = f"**{new_file_name}**"

        if c_thumb:
            ph_path = await client.download_media(c_thumb)
            print(f"Thumbnail downloaded successfully. Path: {ph_path}")
        elif media_type == "video" and message.video.thumbs:
            ph_path = await client.download_media(message.video.thumbs[0].file_id)

        if ph_path:
            Image.open(ph_path).convert("RGB").save(ph_path)
            img = Image.open(ph_path)
            img.resize((1280, 720))
            img.save(ph_path, "JPEG")

        # Use the thread pool for uploading
        upload_future = executor.submit(upload_media, client, message, metadata_path, ph_path, caption, media_type, duration, upload_msg)
        upload_future.result()
        
        await download_msg.delete()
        os.remove(file_path)
        if ph_path:
            os.remove(ph_path)
        if metadata_path:
            os.remove(metadata_path)

        del renaming_operations[file_id]
