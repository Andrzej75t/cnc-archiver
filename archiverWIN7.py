#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CNC Archiver - System śledzenia plików ISO przez proces produkcyjny
Autor: Andrzej (z poprawkami)
"""

import os
import sqlite3
import shutil
import threading
import time
import re
import json
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List, Any
from contextlib import contextmanager

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, END
from PIL import Image, ImageDraw
import pystray

# Opcjonalne importy z fallbackiem
try:
    import chardet
    CHARDET_AVAILABLE = True
except ImportError:
    CHARDET_AVAILABLE = False

# ---------------- KONFIGURACJA ----------------

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(APP_DIR, "config.json")
DB_FILE = os.path.join(APP_DIR, "database.sqlite")
LOG_FILE = os.path.join(APP_DIR, "cnc_archiver.log")

ARCHIVE_DELAY = 7 * 60  # 7 minut
POLL_INTERVAL = 2.0  # sekundy
DB_TIMEOUT = 10

# ---------------- LOGOWANIE ----------------

def setup_logging() -> logging.Logger:
    """Konfiguruje rotujący system logowania."""
    logger = logging.getLogger("cnc_archiver")
    logger.setLevel(logging.DEBUG)
    
    # Format logów
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(threadName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Plik z rotacją (5 MB, 3 kopie)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Konsola (tylko WARNING i wyżej)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

log = setup_logging()

# ---------------- TYPY DANYCH ----------------

@dataclass
class FileRecord:
    """Reprezentacja rekordu pliku w bazie."""
    id: Optional[int]
    nazwa: str
    sciezka_a: str
    sciezka_b: str
    sciezka_c: str
    czas_wrzucenia: datetime
    czas_pobrania: Optional[datetime] = None
    czas_archiwizacji: Optional[datetime] = None
    czas_cyklu_ab: Optional[str] = None
    material: str = "Brak"
    grubosc: str = "Brak"
    czas_realny_cnc: str = "Brak"

# ---------------- KONTEXT MANAGERY ----------------

@contextmanager
def db_connection(timeout: int = DB_TIMEOUT):
    """Context manager dla połączeń SQLite z automatycznym cleanup."""
    conn = sqlite3.connect(DB_FILE, timeout=timeout)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ---------------- UTIL ----------------

def detect_encoding(file_path: str) -> str:
    """Wykrywa kodowanie pliku z fallbackiem."""
    if CHARDET_AVAILABLE:
        try:
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read())
                return result.get("encoding") or "utf-8"
        except Exception as e:
            log.warning("Błąd detekcji kodowania dla %s: %s", file_path, e)
    return "utf-8"

def extract_cnc_time(text: str) -> str:
    """
    Wyciąga czas CNC z różnych formatów.
    Obsługuje: Fanuc, Siemens, Mazak, generyczne.
    """
    patterns = [
        # Fanuc: Time: HH:MM:SS lub Cycle Time: HH:MM:SS
        r'(?:Cycle\s+)?Time\s*[:=]\s*(\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?)',
        # Siemens: ;TIME=HH:MM:SS.MS lub #TIME=HH:MM:SS
        r'[;#]TIME[=:]\s*(\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)',
        # Mazak: (HH:MM:SS) na końcu linii z czasem
        r'(?:CUT\s+TIME|RUN\s+TIME)\s+(\d{1,2}:\d{2}:\d{2})',
        # Generyczny czas w nawiasach
        r'\((\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\)',
        # Czas na końcu linii (ostatnia deska ratunku)
        r'(\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s*$',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
        if match:
            return match.group(1).replace(",", ".").strip()
    
    return "Brak"

def format_timedelta(td: timedelta) -> str:
    """Formatuje timedelta jako HH:MM:SS."""
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def safe_filename(name: str) -> str:
    """Czyści nazwę pliku z niebezpiecznych znaków."""
    return re.sub(r'[<>:\"/\\|?*]', '_', name)

# ---------------- KONFIGURACJA ----------------

class ConfigManager:
    """Zarządzanie konfiguracją aplikacji."""
    
    @staticmethod
    def load() -> Optional[Dict[str, str]]:
        """Wczytuje konfigurację z pliku."""
        if not os.path.exists(CONFIG_FILE):
            log.info("Brak pliku konfiguracyjnego")
            return None
        
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            # Walidacja wymaganych kluczy
            required = {"folder_a", "folder_b", "folder_c", "folder_d"}
            if not required.issubset(config.keys()):
                log.error("Niekompletna konfiguracja: brakujące klucze %s", 
                         required - config.keys())
                return None
            
            # Walidacja ścieżek
            for key, path in config.items():
                if not os.path.isdir(path):
                    log.warning("Ścieżka %s nie istnieje: %s", key, path)
            
            log.info("Wczytano konfigurację")
            return config
            
        except json.JSONDecodeError as e:
            log.error("Błąd parsowania JSON: %s", e)
            return None
        except Exception as e:
            log.exception("Błąd wczytywania konfiguracji")
            return None
    
    @staticmethod
    def save(folder_a: str, folder_b: str, folder_c: str, folder_d: str) -> bool:
        """Zapisuje konfigurację do pliku."""
        config = {
            "folder_a": os.path.normpath(folder_a),
            "folder_b": os.path.normpath(folder_b),
            "folder_c": os.path.normpath(folder_c),
            "folder_d": os.path.normpath(folder_d),
        }
        
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            log.info("Zapisano konfigurację")
            return True
        except Exception as e:
            log.exception("Błąd zapisu konfiguracji")
            return False

# ---------------- BAZA DANYCH ----------------

class Database:
    """Warstwa abstrakcji bazy danych."""
    
    @staticmethod
    def init() -> bool:
        """Inicjalizuje schemat bazy danych."""
        try:
            with db_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS pliki (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nazwa TEXT NOT NULL,
                        sciezka_a TEXT NOT NULL,
                        sciezka_b TEXT NOT NULL,
                        sciezka_c TEXT NOT NULL,
                        czas_wrzucenia TEXT NOT NULL,
                        czas_pobrania TEXT,
                        czas_archiwizacji TEXT,
                        czas_cyklu_ab TEXT,
                        material TEXT DEFAULT 'Brak',
                        grubosc TEXT DEFAULT 'Brak',
                        czas_realny_cnc TEXT DEFAULT 'Brak',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Indeksy dla wydajności
                conn.execute("CREATE INDEX IF NOT EXISTS idx_nazwa ON pliki(nazwa)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_czas ON pliki(czas_archiwizacji)")
                
            log.info("Baza danych zainicjalizowana")
            return True
        except Exception as e:
            log.exception("Błąd inicjalizacji bazy")
            return False
    
    @staticmethod
    def insert(record: FileRecord) -> Optional[int]:
        """Wstawia nowy rekord do bazy."""
        with db_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO pliki 
                (nazwa, sciezka_a, sciezka_b, sciezka_c,
                 czas_wrzucenia, czas_pobrania, czas_archiwizacji,
                 czas_cyklu_ab, material, grubosc, czas_realny_cnc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.nazwa,
                record.sciezka_a,
                record.sciezka_b,
                record.sciezka_c,
                record.czas_wrzucenia.strftime("%Y-%m-%d %H:%M:%S"),
                record.czas_pobrania.strftime("%Y-%m-%d %H:%M:%S") if record.czas_pobrania else None,
                record.czas_archiwizacji.strftime("%Y-%m-%d %H:%M:%S") if record.czas_archiwizacji else None,
                record.czas_cyklu_ab,
                record.material,
                record.grubosc,
                record.czas_realny_cnc
            ))
            return cursor.lastrowid
    
    @staticmethod
    def update_cnc_data(nazwa: str, material: str, grubosc: str, czas_cnc: str) -> bool:
        """Aktualizuje dane CNC dla pliku."""
        with db_connection() as conn:
            cursor = conn.execute("""
                UPDATE pliki 
                SET material = ?, grubosc = ?, czas_realny_cnc = ?
                WHERE nazwa = ?
            """, (material, grubosc, czas_cnc, nazwa))
            return cursor.rowcount > 0
    
    @staticmethod
    def search(nazwa_pattern: str) -> List[sqlite3.Row]:
        """Wyszukuje pliki po nazwie (LIKE)."""
        with db_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM pliki WHERE nazwa LIKE ? ORDER BY czas_archiwizacji DESC",
                (f'%{nazwa_pattern}%',)
            )
            return cursor.fetchall()
    
    @staticmethod
    def get_by_name(nazwa_pattern: str) -> List[sqlite3.Row]:
        """Pobiera rekordy po dokładnej lub częściowej nazwie."""
        with db_connection() as conn:
            cursor = conn.execute(
                "SELECT sciezka_c, nazwa FROM pliki WHERE nazwa LIKE ?",
                (f'%{nazwa_pattern}%',)
            )
            return cursor.fetchall()

# ---------------- SETUP GUI ----------------

class SetupWizard:
    """Kreator pierwszego uruchomienia."""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Konfiguracja CNC Archiver")
        self.root.geometry("550x400")
        self.root.resizable(False, False)
        
        self.folders: Dict[str, tk.StringVar] = {
            k: tk.StringVar(value="Nie wybrano") 
            for k in ["a", "b", "c", "d"]
        }
        self.labels = {
            "a": "Folder A (Incoming / Produkcja)",
            "b": "Folder B (CNC / Wycinanie)",
            "c": "Folder C (Archiwum)",
            "d": "Folder D (DONE TXT / Raporty)"
        }
        
        self._build_ui()
    
    def _build_ui(self):
        """Buduje interfejs kreatora."""
        # Nagłówek
        tk.Label(
            self.root, 
            text="Wybierz foldery dla procesu CNC",
            font=("Segoe UI", 12, "bold")
        ).pack(pady=15)
        
        # Foldery
        for key in ["a", "b", "c", "d"]:
            frame = tk.Frame(self.root)
            frame.pack(fill="x", padx=20, pady=8)
            
            tk.Label(
                frame, 
                text=self.labels[key],
                font=("Segoe UI", 9),
                anchor="w"
            ).pack(fill="x")
            
            btn_frame = tk.Frame(frame)
            btn_frame.pack(fill="x", pady=2)
            
            tk.Label(
                btn_frame,
                textvariable=self.folders[key],
                font=("Consolas", 8),
                fg="gray",
                anchor="w",
                wraplength=350
            ).pack(side="left", fill="x", expand=True)
            
            tk.Button(
                btn_frame,
                text="Wybierz...",
                command=lambda k=key: self._pick_folder(k)
            ).pack(side="right", padx=5)
        
        # Przycisk zapisu
        tk.Button(
            self.root,
            text="Zapisz konfigurację",
            font=("Segoe UI", 10, "bold"),
            bg="#4CAF50",
            fg="white",
            height=2,
            command=self._save
        ).pack(pady=20, fill="x", padx=20)
    
    def _pick_folder(self, key: str):
        """Otwiera dialog wyboru folderu."""
        path = filedialog.askdirectory(title=f"Wybierz: {self.labels[key]}")
        if path:
            self.folders[key].set(os.path.normpath(path))
            log.debug("Wybrano folder %s: %s", key, path)
    
    def _validate(self) -> Optional[Tuple[str, str, str, str]]:
        """Waliduje wybrane ścieżki."""
        paths = {k: v.get() for k, v in self.folders.items()}
        
        # Sprawdź czy wszystkie wybrane
        if any(v == "Nie wybrano" for v in paths.values()):
            messagebox.showerror("Błąd", "Wybierz wszystkie cztery foldery")
            return None
        
        # Sprawdź czy ścieżki istnieją
        for key, path in paths.items():
            if not os.path.isdir(path):
                messagebox.showerror(
                    "Błąd", 
                    f"Folder {key.upper()} nie istnieje:\n{path}"
                )
                return None
        
        # Sprawdź czy foldery są różne
        unique_paths = set(paths.values())
        if len(unique_paths) < 4:
            messagebox.showerror(
                "Błąd",
                "Wszystkie foldery muszą być różne!"
            )
            return None
        
        return (paths["a"], paths["b"], paths["c"], paths["d"])
    
    def _save(self):
        """Zapisuje konfigurację."""
        result = self._validate()
        if result is None:
            return
        
        a, b, c, d = result
        if ConfigManager.save(a, b, c, d):
            messagebox.showinfo("Sukces", "Konfiguracja zapisana!")
            self.root.destroy()
        else:
            messagebox.showerror("Błąd", "Nie udało się zapisać konfiguracji")
    
    def run(self) -> bool:
        """Uruchamia kreator, zwraca True jeśli zapisano."""
        self.root.mainloop()
        return os.path.exists(CONFIG_FILE)

# ---------------- POLLING ENGINE ----------------

class PollingEngine:
    """Silnik monitorowania folderów."""
    
    ISO_PATTERN = re.compile(r'\.iso$', re.IGNORECASE)
    TXT_PATTERN = re.compile(r'\.txt$', re.IGNORECASE)
    ISO_EXTRACT_PATTERN = re.compile(r'#(.+?)\.iso\.txt$', re.IGNORECASE)
    
    def __init__(self, config: Dict[str, str]):
        self.cfg = config
        self.stop_event = threading.Event()
        
        # Stan
        self.waiting: Dict[str, Tuple[str, datetime]] = {}  # iso -> (path_a, time_added)
        self.seen_in_b: Dict[str, datetime] = {}  # iso -> time_first_seen
        self.last_b_time: Optional[datetime] = None
        self.d_cache: Dict[str, float] = {}  # filename -> mtime
        
        # Statystyki
        self.stats = {
            "processed": 0,
            "errors": 0,
            "start_time": datetime.now()
        }
    
    def start(self):
        """Uruchamia silnik w osobnym wątku."""
        self.thread = threading.Thread(target=self._run, name="PollingEngine")
        self.thread.daemon = True
        self.thread.start()
        log.info("Silnik polling uruchomiony")
    
    def stop(self):
        """Zatrzymuje silnik."""
        self.stop_event.set()
        log.info("Zatrzymywanie silnika polling...")
    
    def _run(self):
        """Główna pętla polling."""
        while not self.stop_event.is_set():
            cycle_start = time.monotonic()
            
            try:
                self._process_folder_a()
                self._process_folder_b()
                self._process_folder_d()
                
            except Exception as e:
                log.exception("Błąd w cyklu polling")
                self.stats["errors"] += 1
            
            # Precyzyjne timing
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0, POLL_INTERVAL - elapsed)
            self.stop_event.wait(sleep_time)
        
        log.info("Silnik polling zatrzymany")
    
    def _safe_listdir(self, path: str) -> List[str]:
        """Bezpieczne listowanie folderu."""
        try:
            return os.listdir(path)
        except OSError as e:
            log.error("Nie można odczytać folderu %s: %s", path, e)
            return []
    
    def _process_folder_a(self):
        """Skanuje folder A w poszukiwaniu nowych plików."""
        files = self._safe_listdir(self.cfg["folder_a"])
        
        for filename in files:
            if not self.ISO_PATTERN.search(filename):
                continue
            if filename in self.waiting:
                continue
            
            full_path = os.path.join(self.cfg["folder_a"], filename)
            
            # Tylko zwykłe pliki (nie foldery, nie linki)
            if not os.path.isfile(full_path):
                continue
            
            # Sprawdź czy plik nie jest jeszcze zapisywany
            try:
                initial_size = os.path.getsize(full_path)
                time.sleep(0.1)
                if os.path.getsize(full_path) != initial_size:
                    log.debug("Plik %s wciąż się zapisuje, pomijam", filename)
                    continue
            except OSError:
                continue
            
            self.waiting[filename] = (full_path, datetime.now())
            log.info("Nowy plik w kolejce A: %s", filename)
    
    def _process_folder_b(self):
        """Przetwarza pliki w folderze B i archiwizuje."""
        now = datetime.now()
        
        for filename in list(self.waiting.keys()):
            path_b = os.path.join(self.cfg["folder_b"], filename)
            
            if not os.path.exists(path_b):
                continue
            
            # Pierwsze wykrycie w B
            if filename not in self.seen_in_b:
                self.seen_in_b[filename] = now
                log.info("Plik %s pojawił się w B, start licznika 7min", filename)
                continue
            
            # Sprawdź minęło 7 minut
            elapsed = (now - self.seen_in_b[filename]).total_seconds()
            if elapsed < ARCHIVE_DELAY:
                continue
            
            # Archiwizuj
            self._archive_file(filename, now)
    
    def _archive_file(self, filename: str, now: datetime):
        """Wykonuje atomową archiwizację pliku."""
        path_a, time_added = self.waiting[filename]
        path_c = os.path.join(self.cfg["folder_c"], filename)
        path_b = os.path.join(self.cfg["folder_b"], filename)
        
        temp_path = path_c + ".tmp"
        
        try:
            # 1. Kopia z weryfikacją
            shutil.copy2(path_a, temp_path)
            
            # 2. Weryfikacja integralności
            if not os.path.exists(temp_path):
                raise IOError("Plik tymczasowy nie został utworzony")
            
            if os.path.getsize(temp_path) != os.path.getsize(path_a):
                raise IOError("Rozmiar pliku po kopiowaniu się nie zgadza")
            
            # 3. Atomowy rename
            os.replace(temp_path, path_c)
            
            # 4. Weryfikacja przed usunięciem źródła
            if not os.path.exists(path_c):
                raise IOError("Plik docelowy nie istnieje po rename")
            
            # 5. Usuń źródło
            os.remove(path_a)
            
            # 6. Oblicz czas cyklu
            cycle_time = None
            if self.last_b_time:
                delta = now - self.last_b_time
                cycle_time = format_timedelta(delta)
            self.last_b_time = now
            
            # 7. Zapis do bazy
            record = FileRecord(
                id=None,
                nazwa=filename,
                sciezka_a=path_a,
                sciezka_b=path_b,
                sciezka_c=path_c,
                czas_wrzucenia=time_added,
                czas_pobrania=self.seen_in_b[filename],
                czas_archiwizacji=now,
                czas_cyklu_ab=cycle_time
            )
            
            db_id = Database.insert(record)
            
            # 8. Cleanup stanu
            del self.waiting[filename]
            self.seen_in_b.pop(filename, None)
            
            self.stats["processed"] += 1
            log.info(
                "Zarchiwizowano [%d]: %s (cykl: %s, oczekiwanie: %s)",
                db_id, filename, cycle_time or "N/A",
                format_timedelta(now - time_added)
            )
            
        except Exception as e:
            log.exception("Błąd archiwizacji %s", filename)
            self.stats["errors"] += 1
            
            # Cleanup
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
    
    def _process_folder_d(self):
        """Przetwarza raporty TXT z folderu D."""
        files = self._safe_listdir(self.cfg["folder_d"])
        updates: List[Tuple[str, str, str, str]] = []  # (mat, gru, czas, iso_name)
        
        for filename in files:
            if not self.TXT_PATTERN.search(filename):
                continue
            
            full_path = os.path.join(self.cfg["folder_d"], filename)
            
            # Sprawdź czy plik się zmienił
            try:
                mtime = os.path.getmtime(full_path)
            except OSError:
                continue
            
            if self.d_cache.get(filename) == mtime:
                continue
            self.d_cache[filename] = mtime
            
            # Parsuj nazwę pliku
            match = self.ISO_EXTRACT_PATTERN.search(filename)
            if not match:
                log.debug("Nie udało się sparsować nazwy: %s", filename)
                continue
            
            iso_name = match.group(1) + ".iso"
            
            # Odczytaj zawartość
            try:
                encoding = detect_encoding(full_path)
                with open(full_path, 'r', encoding=encoding, errors='ignore') as f:
                    content = f.read()
            except Exception as e:
                log.error("Błąd odczytu %s: %s", filename, e)
                continue
            
            # Parsuj dane
            material, grubosc = self._parse_material(content)
            cnc_time = extract_cnc_time(content)
            
            updates.append((material, grubosc, cnc_time, iso_name))
            log.debug("Sparsowano %s: mat=%s, gr=%s, czas=%s", 
                     filename, material, grubosc, cnc_time)
        
        # Batch update
        if not updates:
            return
        
        success_count = 0
        for material, grubosc, cnc_time, iso_name in updates:
            if Database.update_cnc_data(iso_name, material, grubosc, cnc_time):
                success_count += 1
        
        if success_count:
            log.info("Zaktualizowano dane CNC dla %d plików", success_count)
    
    def _parse_material(self, text: str) -> Tuple[str, str]:
        """Wyciąga materiał i grubość z pierwszej linii."""
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        
        if not lines:
            return "Brak", "Brak"
        
        first = lines[0]
        
        # Format: "MATERIAL-GRUBOŚĆ" lub "MATERIAL - GRUBOŚĆ"
        # Pomijaj linie zaczynające się od ( - to zazwyczaj komentarze CNC
        if first.startswith('(') or first.startswith(';'):
            # Szukaj w kolejnych liniach
            for line in lines[1:3]:
                if '-' in line and not line.startswith('('):
                    first = line
                    break
            else:
                return "Brak", "Brak"
        
        if '-' not in first:
            return first[:50], "Brak"  # Limit długości
        
        # Podziel tylko na pierwszym myślniku
        parts = first.split("-", 1)
        material = parts[0].strip()[:50]
        grubosc = parts[1].strip()[:20] if len(parts) > 1 else "Brak"
        
        return material, grubosc
    
    def get_stats(self) -> Dict[str, Any]:
        """Zwraca statystyki silnika."""
        return {
            **self.stats,
            "waiting_count": len(self.waiting),
            "in_b_count": len(self.seen_in_b),
            "uptime": format_timedelta(datetime.now() - self.stats["start_time"])
        }

# ---------------- GŁÓWNE GUI ----------------

class MainWindow:
    """Główne okno aplikacji."""
    
    def __init__(self, config: Dict[str, str], engine: PollingEngine):
        self.cfg = config
        self.engine = engine
        
        self.root = tk.Tk()
        self.root.title("CNC Archiver by Andrzej")
        self.root.geometry("900x600")
        self.root.minsize(700, 400)
        
        # Ikona zasobnika
        self._setup_tray_icon()
        
        # Buduj UI
        self._build_ui()
        
        # Timer aktualizacji statystyk
        self._schedule_stats_update()
    
    def _build_ui(self):
        """Buduje interfejs głównego okna."""
        # Górny panel - wyszukiwanie
        top_frame = tk.Frame(self.root, bg="#f0f0f0")
        top_frame.pack(fill="x", padx=10, pady=10)
        
        tk.Label(
            top_frame, 
            text="Szukaj:", 
            bg="#f0f0f0",
            font=("Segoe UI", 10)
        ).pack(side="left", padx=5)
        
        self.search_var = tk.StringVar()
        self.search_entry = tk.Entry(
            top_frame, 
            textvariable=self.search_var,
            width=40,
            font=("Consolas", 11)
        )
        self.search_entry.pack(side="left", padx=5)
        self.search_entry.bind("<Return>", lambda e: self._search())
        
        tk.Button(
            top_frame,
            text="Szukaj",
            command=self._search,
            bg="#2196F3",
            fg="white",
            font=("Segoe UI", 9, "bold"),
            width=12
        ).pack(side="left", padx=5)
        
        tk.Button(
            top_frame,
            text="Przywróć do A",
            command=self._restore,
            bg="#FF9800",
            fg="white",
            font=("Segoe UI", 9, "bold"),
            width=14
        ).pack(side="left", padx=5)
        
        # Panel statystyk
        self.stats_frame = tk.LabelFrame(
            self.root,
            text="Statystyki systemu",
            font=("Segoe UI", 9, "bold")
        )
        self.stats_frame.pack(fill="x", padx=10, pady=5)
        
        self.stats_label = tk.Label(
            self.stats_frame,
            text="Inicjalizacja...",
            font=("Consolas", 9),
            justify="left"
        )
        self.stats_label.pack(anchor="w", padx=10, pady=5)
        
        # Główna tabela - Treeview zamiast Text
        table_frame = tk.Frame(self.root)
        table_frame.pack(expand=True, fill="both", padx=10, pady=5)
        
        # Scrollbary
        vsb = ttk.Scrollbar(table_frame, orient="vertical")
        hsb = ttk.Scrollbar(table_frame, orient="horizontal")
        
        # Treeview
        columns = ("nazwa", "wrzucony", "w_b", "zarchiwizowany", "cykl", "material", "grubosc", "cnc")
        self.tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
            height=15
        )
        
        vsb.config(command=self.tree.yview)
        hsb.config(command=self.tree.xview)
        
        # Nagłówki
        headers = {
            "nazwa": "Nazwa pliku",
            "wrzucony": "Wrzucony do A",
            "w_b": "Pobrany do B",
            "zarchiwizowany": "Zarchiwizowany",
            "cykl": "Cykl A→B",
            "material": "Materiał",
            "grubosc": "Grubość",
            "cnc": "Czas CNC"
        }
        
        widths = {
            "nazwa": 200,
            "wrzucony": 130,
            "w_b": 130,
            "zarchiwizowany": 130,
            "cykl": 80,
            "material": 100,
            "grubosc": 80,
            "cnc": 80
        }
        
        for col in columns:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], minwidth=50)
        
        # Układ
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        self.tree.pack(expand=True, fill="both")
        
        # Podwójny klik - szczegóły
        self.tree.bind("<Double-1>", self._show_details)
        
        # Pasek statusu
        self.status_var = tk.StringVar(value="Gotowy")
        status_bar = tk.Label(
            self.root,
            textvariable=self.status_var,
            bd=1,
            relief=tk.SUNKEN,
            anchor="w",
            font=("Segoe UI", 9)
        )
        status_bar.pack(side="bottom", fill="x")
        
        # Obsługa zamykania
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
    
    def _search(self):
        """Wykonuje wyszukiwanie w bazie."""
        pattern = self.search_var.get().strip()
        if not pattern:
            messagebox.showwarning("Uwaga", "Wpisz frazę do wyszukania")
            return
        
        self.status_var.set(f"Wyszukiwanie: '{pattern}'...")
        self.root.update()
        
        try:
            results = Database.search(pattern)
            self._display_results(results)
            self.status_var.set(f"Znaleziono {len(results)} wyników")
        except Exception as e:
            log.exception("Błąd wyszukiwania")
            messagebox.showerror("Błąd", f"Nie udało się wyszukać:\n{e}")
            self.status_var.set("Błąd wyszukiwania")
    
    def _display_results(self, rows: List[sqlite3.Row]):
        """Wyświetla wyniki w tabeli."""
        # Wyczyść
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Dodaj wyniki
        for row in rows:
            self.tree.insert("", "end", values=(
                row["nazwa"],
                row["czas_wrzucenia"] or "-",
                row["czas_pobrania"] or "-",
                row["czas_archiwizacji"] or "-",
                row["czas_cyklu_ab"] or "-",
                row["material"],
                row["grubosc"],
                row["czas_realny_cnc"]
            ))
    
    def _restore(self):
        """Przywraca plik z archiwum do folderu A."""
        pattern = self.search_var.get().strip()
        if not pattern:
            messagebox.showwarning("Uwaga", "Wpisz nazwę pliku do przywrócenia")
            return
        
        # Potwierdzenie
        if not messagebox.askyesno(
            "Potwierdzenie",
            f"Przywrócić pliki pasujące do '{pattern}' do folderu A?\n\n"
            f"Folder docelowy: {self.cfg['folder_a']}"
        ):
            return
        
        try:
            rows = Database.get_by_name(pattern)
            if not rows:
                messagebox.showinfo("Info", "Nie znaleziono plików w archiwum")
                return
            
            restored = 0
            failed = 0
            
            for path_c, nazwa in rows:
                if not os.path.exists(path_c):
                    log.warning("Plik w bazie nie istnieje na dysku: %s", path_c)
                    failed += 1
                    continue
                
                path_a = os.path.join(self.cfg["folder_a"], nazwa)
                
                # Sprawdź czy nie nadpiszemy istniejącego
                if os.path.exists(path_a):
                    base, ext = os.path.splitext(nazwa)
                    path_a = os.path.join(
                        self.cfg["folder_a"],
                        f"{base}_restored_{datetime.now():%Y%m%d_%H%M%S}{ext}"
                    )
                
                shutil.copy2(path_c, path_a)
                restored += 1
                log.info("Przywrócono: %s -> %s", path_c, path_a)
            
            msg = f"Przywrócono: {restored}"
            if failed:
                msg += f"\nNie znaleziono: {failed}"
            
            messagebox.showinfo("Wynik", msg)
            self.status_var.set(f"Przywrócono {restored} plików")
            
        except Exception as e:
            log.exception("Błąd przywracania")
            messagebox.showerror("Błąd", f"Nie udało się przywrócić:\n{e}")
    
    def _show_details(self, event):
        """Pokazuje szczegóły po podwójnym kliknięciu."""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        values = item["values"]
        
        detail_window = tk.Toplevel(self.root)
        detail_window.title(f"Szczegóły: {values[0]}")
        detail_window.geometry("500x300")
        
        text = tk.Text(detail_window, wrap="word", padx=10, pady=10)
        text.pack(expand=True, fill="both")
        
        labels = [
            "Nazwa pliku", "Wrzucony do A", "Pobrany do B",
            "Zarchiwizowany", "Cykl A→B", "Materiał", "Grubość", "Czas CNC"
        ]
        
        for label, value in zip(labels, values):
            text.insert("end", f"{label}:\n", "bold")
            text.insert("end", f"  {value}\n\n")
        
        text.tag_config("bold", font=("Segoe UI", 10, "bold"))
        text.config(state="disabled")
        
        tk.Button(
            detail_window,
            text="Zamknij",
            command=detail_window.destroy
        ).pack(pady=10)
    
    def _schedule_stats_update(self):
        """Planuje aktualizację statystyk."""
        self._update_stats()
        self.root.after(5000, self._schedule_stats_update)  # co 5 sekund
    
    def _update_stats(self):
        """Aktualizuje wyświetlane statystyki."""
        stats = self.engine.get_stats()
        
        text = (
            f"Uptime: {stats['uptime']} | "
            f"Przetworzone: {stats['processed']} | "
            f"Błędy: {stats['errors']} | "
            f"W kolejce A: {stats['waiting_count']} | "
            f"W oczekiwaniu B: {stats['in_b_count']}"
        )
        
        self.stats_label.config(text=text)
        
        # Kolor ostrzegawczy przy błędach
        if stats['errors'] > 0:
            self.stats_label.config(fg="red")
        else:
            self.stats_label.config(fg="black")
    
    def _setup_tray_icon(self):
        """Przygotowuje ikonę w zasobniku systemowym."""
        self.tray_icon = None
        self.tray_menu = pystray.Menu(
            pystray.MenuItem("Pokaż", self._show_from_tray),
            pystray.MenuItem("Zamknij", self._quit_from_tray)
        )
    
    def _create_icon_image(self):
        """Tworzy obraz ikony."""
        img = Image.new("RGB", (64, 64), "#1a237e")
        draw = ImageDraw.Draw(img)
        # Prosta grafika CNC
        draw.rectangle([8, 20, 56, 44], fill="#3949ab")
        draw.rectangle([16, 12, 48, 52], outline="white", width=2)
        draw.line([32, 12, 32, 52], fill="white", width=2)
        draw.line([16, 32, 48, 32], fill="white", width=2)
        return img
    
    def start_tray(self):
        """Uruchamia ikonę w zasobniku."""
        self.tray_icon = pystray.Icon(
            "cnc_archiver",
            self._create_icon_image(),
            "CNC Archiver - Aktywny",
            self.tray_menu
        )
        threading.Thread(target=self.tray_icon.run, daemon=True).start()
    
    def _show_from_tray(self):
        """Przywraca okno z zasobnika."""
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
    
    def _quit_from_tray(self):
        """Zamyka aplikację z zasobnika."""
        self._on_close()
    
    def _on_close(self):
        """Obsługa zamykania okna."""
        # Minimalizuj do zasobnika zamiast zamykać
        self.root.withdraw()
        if self.tray_icon:
            self.tray_icon.notify(
                "CNC Archiver działa w tle",
                "Kliknij ikonę w zasobniku, aby przywrócić okno."
            )
    
    def run(self):
        """Uruchamia główną pętlę."""
        self.root.mainloop()

# ---------------- MAIN ----------------

def main():
    """Główna funkcja aplikacji."""
    log.info("=" * 50)
    log.info("CNC Archiver - Start aplikacji")
    log.info("Wersja Python: %s", sys.version)
    log.info("Katalog aplikacji: %s", APP_DIR)
    
    # Sprawdź konfigurację
    config = ConfigManager.load()
    
    if not config:
        log.info("Pierwsze uruchomienie - uruchamiam kreatora")
        wizard = SetupWizard()
        if not wizard.run():
            log.error("Konfiguracja przerwana")
            return
        
        config = ConfigManager.load()
        if not config:
            log.error("Nie udało się wczytać konfiguracji po kreatorze")
            return
    
    log.info("Konfiguracja: %s", {k: v for k, v in config.items()})
    
    # Inicjalizuj bazę
    if not Database.init():
        messagebox.showerror(
            "Błąd krytyczny",
            "Nie udało się zainicjalizować bazy danych.\n"
            "Sprawdź uprawnienia do katalogu:\n" + APP_DIR
        )
        return
    
    # Uruchom silnik polling
    engine = PollingEngine(config)
    engine.start()
    
    # Uruchom GUI
    try:
        app = MainWindow(config, engine)
        app.start_tray()
        app.run()
    finally:
        # Cleanup
        log.info("Zamykanie aplikacji...")
        engine.stop()
        if engine.thread.is_alive():
            engine.thread.join(timeout=5)
        log.info("Aplikacja zamknięta")

if __name__ == "__main__":
    main()
