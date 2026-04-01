import os
import librosa
import numpy as np
import json
from pathlib import Path

def _safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)

def extract_features(file_path):
    """
    提取音频特征，包括整体特征以及首尾15s的过渡特征
    """
    try:
        # 获取总时长
        duration = librosa.get_duration(path=file_path)
        
        # 提取前30s用于整体特征
        y_main, sr = librosa.load(file_path, duration=min(30, duration), sr=22050)
        
        # 提取首尾15s用于过渡分析
        y_start, _ = librosa.load(file_path, duration=min(15, duration), sr=22050)
        offset_end = max(0, duration - 15)
        y_end, _ = librosa.load(file_path, offset=offset_end, duration=15, sr=22050)
        tail_scan_window_sec = min(40, duration)
        offset_tail_scan = max(0, duration - tail_scan_window_sec)
        y_tail_scan, _ = librosa.load(file_path, offset=offset_tail_scan, duration=tail_scan_window_sec, sr=22050)

        end_len_sec = len(y_end) / sr if len(y_end) > 0 else 0.0
        end_rms = librosa.feature.rms(y=y_end)[0] if len(y_end) > 0 else np.array([])
        if end_rms.size > 0:
            peak_rms = float(np.max(end_rms))
            active_threshold = max(peak_rms * 0.2, 1e-4)
            active_indices = np.where(end_rms >= active_threshold)[0]
            if active_indices.size > 0:
                last_active_idx = int(active_indices[-1])
                last_active_sec = librosa.frames_to_time(last_active_idx, sr=sr)
            else:
                last_active_sec = 0.0
            tail_silence_sec = max(0.0, end_len_sec - last_active_sec)
            end_activity_ratio = float(np.clip((end_len_sec - tail_silence_sec) / max(end_len_sec, 1e-6), 0.0, 1.0))
        else:
            tail_silence_sec = 0.0
            end_activity_ratio = 1.0

        mix_lead_sec = float(np.clip(9.1 + tail_silence_sec * 0.62, 7.4, 13.2))
        mix_breath_sec = float(np.clip(1.75 + tail_silence_sec * 0.22, 1.35, 3.2))
        
        def get_metrics(y):
            if len(y) == 0:
                return 0, 0, 0
            if sr <= 0:
                return 0, 0, 0
            bpm = 0.0
            try:
                tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
                bpm = float(tempo[0]) if isinstance(tempo, np.ndarray) else float(tempo)
            except Exception:
                bpm = 0.0
            try:
                rms = librosa.feature.rms(y=y)
                energy = float(np.nan_to_num(np.mean(rms), nan=0.0, posinf=0.0, neginf=0.0))
            except Exception:
                energy = 0.0
            try:
                centroid = librosa.feature.spectral_centroid(y=y, sr=sr)
                brightness = float(np.nan_to_num(np.mean(centroid), nan=0.0, posinf=0.0, neginf=0.0))
            except Exception:
                brightness = 0.0
            bpm = float(np.nan_to_num(bpm, nan=0.0, posinf=0.0, neginf=0.0))
            return bpm, energy, brightness
            
        mix_window_samples = int(sr * 10)
        y_start_mix = y_start[:mix_window_samples] if mix_window_samples > 0 else y_start
        y_end_mix = y_end[-mix_window_samples:] if mix_window_samples > 0 and len(y_end) > mix_window_samples else y_end

        def estimate_invalid_tail_sec(y_tail, sample_rate):
            if len(y_tail) == 0:
                return 0.0
            tail_rms = librosa.feature.rms(y=y_tail)[0]
            if tail_rms.size == 0:
                return 0.0
            smooth_kernel = np.ones(5, dtype=np.float32) / 5.0
            tail_rms_smooth = np.convolve(tail_rms, smooth_kernel, mode='same')
            peak_rms = float(np.max(tail_rms))
            if peak_rms <= 1e-7:
                return float(len(y_tail) / sample_rate)
            floor_rms = float(np.percentile(tail_rms_smooth, 40))
            active_threshold = max(peak_rms * 0.065, floor_rms * 0.62, 4e-5)
            active_indices = np.where(tail_rms_smooth >= active_threshold)[0]
            tail_len_sec = float(len(y_tail) / sample_rate)
            if active_indices.size == 0:
                invalid_tail = tail_len_sec
            else:
                last_active_sec = float(librosa.frames_to_time(int(active_indices[-1]), sr=sample_rate))
                invalid_tail = max(0.0, tail_len_sec - last_active_sec)
            if invalid_tail < 0.4:
                return 0.0
            return float(min(40.0, invalid_tail))

        invalid_tail_sec = estimate_invalid_tail_sec(y_tail_scan, sr)
        mix_effect_start_sec = float(np.clip(10.0 + invalid_tail_sec, 8.0, 50.0))
        mix_entry_sec = float(np.clip(4.0 + invalid_tail_sec, 4.0, 44.0))

        dynamic_window_sec = float(np.clip(10.0 + invalid_tail_sec * 0.55, 8.0, 24.0))
        dynamic_window_samples = int(sr * dynamic_window_sec)
        effective_tail_samples = int(sr * invalid_tail_sec)
        y_for_end_dynamic = y_tail_scan[:-effective_tail_samples] if effective_tail_samples > 0 and len(y_tail_scan) > effective_tail_samples else y_tail_scan
        if dynamic_window_samples > 0 and len(y_for_end_dynamic) > dynamic_window_samples:
            y_end_dynamic = y_for_end_dynamic[-dynamic_window_samples:]
        else:
            y_end_dynamic = y_for_end_dynamic
        y_start_dynamic = y_start[:dynamic_window_samples] if dynamic_window_samples > 0 else y_start

        bpm, energy, brightness = get_metrics(y_main)
        start_bpm, start_energy, _ = get_metrics(y_start)
        end_bpm, end_energy, _ = get_metrics(y_end)
        _, start_10s_energy, _ = get_metrics(y_start_mix)
        _, end_10s_energy, _ = get_metrics(y_end_mix)
        _, start_dynamic_energy, _ = get_metrics(y_start_dynamic)
        _, end_dynamic_energy, _ = get_metrics(y_end_dynamic)
        
        return {
            "duration_sec": round(float(duration), 3),
            "bpm": bpm,
            "energy": energy,
            "brightness": brightness,
            "start_bpm": start_bpm,
            "start_energy": start_energy,
            "start_10s_energy": start_10s_energy,
            "end_bpm": end_bpm,
            "end_energy": end_energy,
            "end_10s_energy": end_10s_energy,
            "start_dynamic_energy": start_dynamic_energy,
            "end_dynamic_energy": end_dynamic_energy,
            "dynamic_window_sec": round(dynamic_window_sec, 3),
            "tail_silence_sec": round(tail_silence_sec, 3),
            "tail_scan_window_sec": round(float(tail_scan_window_sec), 3),
            "invalid_tail_sec": round(invalid_tail_sec, 3),
            "end_activity_ratio": round(end_activity_ratio, 3),
            "mix_lead_sec": round(mix_lead_sec, 3),
            "mix_breath_sec": round(mix_breath_sec, 3),
            "mix_effect_start_sec": round(mix_effect_start_sec, 3),
            "mix_entry_sec": round(mix_entry_sec, 3)
        }
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def analyze_directory(directory_path, cache_file=None, force_reanalyze=False, include_filenames=None):
    """
    扫描目录中的音频文件并提取特征，支持缓存以节省时间
    如果 force_reanalyze 为 True，将忽略现有缓存并重新分析
    """
    if cache_file is None:
        # Default fallback
        IS_VERCEL = os.environ.get("VERCEL") == "1"
        cache_file = "/tmp/analysis_cache.json" if IS_VERCEL else "analysis_cache.json"

    supported_formats = ['.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg']
    results = []
    
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        return []
        
    include_set = None
    if include_filenames is not None:
        include_set = {str(name).casefold() for name in include_filenames if str(name).strip()}

    # 获取目录下的所有音频文件
    current_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext in supported_formats:
                if include_set is not None and file.casefold() not in include_set:
                    continue
                current_files.append(os.path.join(root, file))
                
    # 加载缓存
    cached_dict = {}
    cached_data_raw = []
    if not force_reanalyze and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                if isinstance(cached_data, dict):
                    cached_data = list(cached_data.values())
                if not isinstance(cached_data, list):
                    cached_data = []
                cached_data_raw = [item for item in cached_data if isinstance(item, dict)]
                for item in cached_data:
                    if not isinstance(item, dict):
                        continue
                    cached_filename = str(item.get('filename') or '')
                    if include_set is not None and cached_filename.casefold() not in include_set:
                        continue
                    # 使用 filename 作为键，如果有 mtime 则更好
                    cached_dict[cached_filename] = item
        except json.JSONDecodeError:
            print("Cache file corrupted, starting fresh...")

    for file_path in current_files:
        file = os.path.basename(file_path)
        mtime = os.path.getmtime(file_path)
        file_size_bytes = os.path.getsize(file_path)
        
        # 检查缓存是否有效 (文件名存在且修改时间匹配)
        cached_item = cached_dict.get(file)
        if cached_item and cached_item.get('mtime') == mtime:
            cached_item['bpm'] = _safe_float(cached_item.get('bpm'), 0.0)
            cached_item['energy'] = _safe_float(cached_item.get('energy'), 0.0)
            cached_item['brightness'] = _safe_float(cached_item.get('brightness'), 0.0)
            duration_sec = _safe_float(cached_item.get('duration_sec'), 0.0)
            cached_item['duration_sec'] = duration_sec
            if not cached_item.get('file_size_bytes'):
                cached_item['file_size_bytes'] = int(file_size_bytes)
            if not cached_item.get('source_bitrate_kbps_est'):
                if duration_sec > 0:
                    cached_item['source_bitrate_kbps_est'] = round((file_size_bytes * 8.0) / duration_sec / 1000.0, 2)
                else:
                    cached_item['source_bitrate_kbps_est'] = 0.0
            print(f"Using cached features for {file}...")
            results.append(cached_item)
        else:
            print(f"Analyzing {file}...")
            features = extract_features(file_path)
            if features:
                duration_sec = float(features.get("duration_sec") or 0.0)
                source_bitrate_kbps_est = round((file_size_bytes * 8.0) / duration_sec / 1000.0, 2) if duration_sec > 0 else 0.0
                results.append({
                    "id": len(results) + 1,
                    "filename": file,
                    "path": file_path,
                    "name": os.path.splitext(file)[0],
                    "mtime": mtime,
                    "file_size_bytes": int(file_size_bytes),
                    "source_bitrate_kbps_est": source_bitrate_kbps_est,
                    **features
                })
    
    # 重新分配 ID 保证连续性
    for idx, r in enumerate(results):
        r['id'] = idx + 1
        
    # 归一化特征 (0-1) 方便后续计算情绪值
    if results:
        for r in results:
            r['bpm'] = _safe_float(r.get('bpm'), 0.0)
            r['energy'] = _safe_float(r.get('energy'), 0.0)
            r['brightness'] = _safe_float(r.get('brightness'), 0.0)
        max_bpm = max((r.get('bpm') or 0.0) for r in results) or 1
        max_energy = max((r.get('energy') or 0.0) for r in results) or 1
        max_brightness = max((r.get('brightness') or 0.0) for r in results) or 1
        
        for r in results:
            r['bpm_norm'] = r['bpm'] / max_bpm
            r['energy_norm'] = r['energy'] / max_energy
            r['brightness_norm'] = r['brightness'] / max_brightness
            # 综合情绪值: 假设高BPM和高能量代表高昂的情绪
            r['emotion_score'] = (r['bpm_norm'] * 0.4 + r['energy_norm'] * 0.4 + r['brightness_norm'] * 0.2)
            r['emotion_score'] = round(r['emotion_score'] * 100, 2)

    cache_to_write = results
    if include_set is not None and cached_data_raw:
        keep_old = []
        include_name_set = {str(name).casefold() for name in include_filenames if str(name).strip()}
        for item in cached_data_raw:
            cached_filename = str(item.get('filename') or '')
            if not cached_filename:
                continue
            if cached_filename.casefold() in include_name_set:
                continue
            keep_old.append(item)
        merged_cache_map = {str(item.get("filename")): item for item in keep_old if item.get("filename")}
        for item in results:
            merged_cache_map[str(item.get("filename"))] = item
        cache_to_write = sorted(merged_cache_map.values(), key=lambda x: str(x.get("filename", "")).casefold())

    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache_to_write, f, ensure_ascii=False, indent=2)
        
    return results

def generate_ideal_curve(num_songs):
    """
    生成一个理想的演唱会情绪曲线（基于著名的“优秀兴趣曲线” / Interest Curve）
    特点：开场Hook -> 谷底 -> 逐步上升的多个波峰与波谷 -> 最终高潮(Climax) -> 迅速收尾
    """
    if num_songs == 0:
        return []
        
    # 时间节点 (0.0 - 1.0 比例)
    timeline_norm = [0.0, 0.15, 0.22, 0.32, 0.40, 0.55, 0.65, 0.80, 1.0]
    # 设定的对应情绪值 (0.0 - 1.0 比例)
    emotions = [0.1, 0.6, 0.25, 0.45, 0.35, 0.75, 0.4, 1.0, 0.05]
    
    # 为每首歌生成对应的位置 (0 到 1)
    if num_songs == 1:
        x = [0.5]
    else:
        x = np.linspace(0, 1, num_songs)
        
    # 插值计算理想情绪值
    ideal_scores = np.interp(x, timeline_norm, emotions)
    
    # 映射到 10-90 的分数区间
    normalized_curve = ideal_scores * 80 + 10
    return normalized_curve.tolist()

def sort_playlist(songs):
    """
    考虑目标情绪曲线和前后歌曲的过渡（BPM与能量）
    """
    if not songs:
        return [], [], []
        
    num_songs = len(songs)
    ideal_scores = generate_ideal_curve(num_songs)
    
    # 为了保证每首歌都能排进去且不重复，我们维护一个 remaining_songs 列表
    remaining_songs = list(songs)
    sorted_playlist = []
    
    # 选第一首歌：最接近第一个理想情绪值的歌
    first_song = min(remaining_songs, key=lambda s: abs(s['emotion_score'] - ideal_scores[0]))
    sorted_playlist.append(first_song)
    remaining_songs.remove(first_song)
    
    for i in range(1, num_songs):
        target_emotion = ideal_scores[i]
        prev_song = sorted_playlist[-1]
        
        # 获取上一首歌的结尾特征 (兼容旧缓存没有 end_bpm 的情况)
        prev_end_bpm = prev_song.get('end_bpm', prev_song.get('bpm', 120))
        prev_end_energy = prev_song.get('end_energy', prev_song.get('energy', 0.5))
        
        best_song = None
        best_cost = float('inf')
        
        for candidate in remaining_songs:
            cand_start_bpm = candidate.get('start_bpm', candidate.get('bpm', 120))
            cand_start_energy = candidate.get('start_energy', candidate.get('energy', 0.5))
            cand_emotion = candidate['emotion_score']
            
            # 1. 情绪差异权重
            emotion_diff = abs(cand_emotion - target_emotion)
            
            # 2. BPM差异：允许直接接，或者倍速/半速接（比如60接120）
            # 计算比例差异
            max_bpm = max(cand_start_bpm, prev_end_bpm)
            min_bpm = max(min(cand_start_bpm, prev_end_bpm), 1)
            bpm_ratio = max_bpm / min_bpm
            # 如果接近 1 或者 2，则认为是好衔接
            bpm_diff = min(abs(bpm_ratio - 1), abs(bpm_ratio - 2)) * 100
            
            # 3. 能量差异
            # 能量一般在0-1之间，放大差异
            energy_diff = abs(cand_start_energy - prev_end_energy) * 50
            
            # 综合Cost：情绪贴合曲线最重要，其次是BPM衔接，再是能量平滑
            cost = emotion_diff * 1.5 + bpm_diff * 1.0 + energy_diff * 0.5
            
            if cost < best_cost:
                best_cost = cost
                best_song = candidate
                
        sorted_playlist.append(best_song)
        remaining_songs.remove(best_song)
        
    actual_scores = [song['emotion_score'] for song in sorted_playlist]
    return sorted_playlist, actual_scores, ideal_scores
