import pygame
from map import Map

def main():
    # Initialize Pygame
    pygame.init()

    # Create the screen
    screen = pygame.display.set_mode((600, 600))
    pygame.display.set_caption("Map Test")

    # Create the map instance
    city_map = Map()

    # Set some taxi positions for testing
    city_map.set_position(2, 3, 1)  # Taxi at (2, 3)
    city_map.set_position(5, 5, 1)  # Taxi at (5, 5)
    city_map.set_position(10, 10, 1)  # Taxi at (10, 10)

    # Main loop
    running = True
    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        # Draw the map
        city_map.draw(screen)

    pygame.quit()

if __name__ == "__main__":
    main()
