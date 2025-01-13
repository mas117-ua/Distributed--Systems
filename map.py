import pygame


class Map:
    def __init__(self, width=600, height=600):
        self.width = width
        self.height = height
        self.cell_size = 30  # Size of each cell
        self.columns = 20  # Number of columns
        self.rows = 20  # Number of rows
        self.map_data = bytearray(self.rows * self.columns)  # Array of bytes for the map
        self.taxi_ids = {}  # Dictionary to store taxi IDs by their positions

        pygame.font.init()
        self.font = pygame.font.SysFont(None, 18)

    def set_position(self, x, y, value,taxi_id = None):
        """Set the value at position (x, y) of the map."""
        index = y * self.columns + x
        self.map_data[index] = value  # value must be 0 (empty) or 1 (taxi)
        print(f"Set position ({x + 1}, {y + 1}) to {value}")  # A 1 significa que hay un taxi

        if taxi_id:
            self.taxi_ids[(x, y)] = taxi_id  # Store the taxi ID by its position

    def draw(self, screen, taxis):
        """Draw the grid and all taxis."""
        print(f"Map data: {list(self.map_data)}")  # Agrega esta línea para ver los valores del mapa

        screen.fill((255, 255, 255))  # White background

        # Draw grid
        for row in range(self.rows):
            for col in range(self.columns):
                rect = pygame.Rect(col * self.cell_size, row * self.cell_size, self.cell_size, self.cell_size)
                pygame.draw.rect(screen, (200, 200, 200), rect, 1)  # Light grey grid lines

                # Check if there's a taxi in this position
                if self.map_data[row * self.columns + col] == 1:  # 1 means taxi
                    pygame.draw.rect(screen, (0, 255, 0), rect)  # Green for taxis

                    # Find taxi info by position
                    taxi = next((taxi for taxi in taxis if taxi['POS'] == f"{col},{row}"), None)
                    if taxi:
                        taxi_id = taxi['Id']
                        text_surface = self.font.render(taxi_id, True, (0, 0, 0))  # Taxi ID in black
                        text_rect = text_surface.get_rect(center=rect.center)
                        screen.blit(text_surface, text_rect)  # Draw ID at the center of the cell

        pygame.display.flip()  # Refresh the screen
